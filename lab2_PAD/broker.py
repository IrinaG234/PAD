"""Persistent, bounded delivery with one outstanding message per subscriber."""
import argparse
import copy
import json
import os
import random
import threading
import time
import uuid
from pathlib import Path
import common as config
from common import ContractError, create, utcnow, validate, validate_subject

HOST=config.HOST; DEFAULT_PORT=config.PORT; STATE_FILE=config.STATE_FILE
MAX_ATTEMPTS=config.MAX_ATTEMPTS; ACK_TIMEOUT=config.ACK_TIMEOUT
lock=threading.RLock(); online_subs={}; online_pubs={}; running=False; storage_ok=True

def empty_state():
    return dict(next_id=1,publishers=[],subscribers={},history=[],dlq=[],accepted={},topics=list(config.TOPICS))

state=empty_state()

def log(event,message=None,result='ok',error=None,duration=0):
    m=message or {}
    row=dict(timestamp=utcnow(),level='ERROR' if error else 'INFO',component='broker',
             event=event,correlationId=m.get('correlationId'),messageId=m.get('messageId'),
             result=result,durationMs=round(duration*1000,3),error=error)
    try:
        with open(config.LOG_FILE,'a',encoding='utf-8') as out: out.write(json.dumps(row)+'\n')
    except OSError: pass

def record(event,sid=None,message=None,detail=''):
    state['history'].append(dict(time=utcnow(),event=event,subscriber=sid,
                                message=copy.deepcopy(message),detail=detail))

def save():
    global storage_ok
    started=time.monotonic()
    try:
        with open(str(STATE_FILE)+'.tmp','w',encoding='utf-8') as out:
            json.dump(state,out,ensure_ascii=False,indent=2); out.flush(); os.fsync(out.fileno())
        os.replace(str(STATE_FILE)+'.tmp',STATE_FILE); storage_ok=True
    except OSError:
        storage_ok=False
        log('PERSIST_FAILED',result='unavailable',error='STORAGE_UNAVAILABLE',duration=time.monotonic()-started)
        raise

def commit(change):
    global state
    before=copy.deepcopy(state); count=len(state['history']); started=time.monotonic()
    try:
        result=change(); save()
    except Exception:
        state=before; raise
    for row in state['history'][count:]: log(row['event'],row['message'],duration=time.monotonic()-started)
    return result

def load():
    global state
    path=Path(STATE_FILE)
    if path.exists() and path.stat().st_size:
        state=json.loads(path.read_text(encoding='utf-8-sig'))
        if not isinstance(state,dict) or not isinstance(state.get('subscribers'),dict):
            raise ValueError('Invalid state; restore backup before starting')
    else: state=empty_state()
    for key,default in empty_state().items(): state.setdefault(key,default)
    for sub in state['subscribers'].values():
        for m in sub['pending']:
            if 'messageId' not in m:
                m.update(create(m['text'],message_id=str(uuid.uuid5(uuid.NAMESPACE_URL,'legacy:'+str(m['id'])))))
                m['time']=m['occurredAt']
            m.setdefault('attempts',0); m.setdefault('nextAttemptAt',0)
    save()


import queue
import grpc
from concurrent import futures
import broker_pb2 as pb
import broker_pb2_grpc as rpc

def retry_delay(attempt):
    base=min(config.BACKOFF_CAP,config.BACKOFF_BASE*2**max(0,attempt-1))
    return min(config.BACKOFF_CAP,base*random.uniform(1-config.JITTER,1+config.JITTER))

def dead_letter(sid,message,reason):
    sub=state['subscribers'][sid]
    sub['pending']=[m for m in sub['pending'] if m['messageId']!=message['messageId']]
    state['dlq'].append(dict(subscriber=sid,message=copy.deepcopy(message),reason=reason,time=utcnow()))
    record('DEAD_LETTER',sid,message,reason)

def flush(sid):
    with lock:
        conn=online_subs.get(sid); sub=state['subscribers'].get(sid)
        if not conn or not sub or not sub['pending']: return
        m=sub['pending'][0]
        if time.time()<m.get('nextAttemptAt',0): return
        if m.get('attempts',0)>=MAX_ATTEMPTS:
            commit(lambda:dead_letter(sid,m,'ACK_TIMEOUT')); return
        def attempt():
            m['attempts']=m.get('attempts',0)+1
            m['nextAttemptAt']=time.time()+ACK_TIMEOUT+retry_delay(m['attempts'])
            record('DELIVERY_ATTEMPT',sid,m)
        commit(attempt)
        try: conn.deliver(m)
        except OSError: log('SEND_FAILED',m,'retry_scheduled','TRANSPORT_UNAVAILABLE')

def retry_worker(stop):
    while not stop.wait(0.1):
        with lock: subscribers=list(online_subs)
        for sid in subscribers:
            try: flush(sid)
            except OSError: pass

def publish(name,message):
    validate(message); mid=message['messageId']; existing=state['accepted'].get(mid)
    if existing:
        if existing['publisher']!=name or existing['contract']!=message: raise ContractError('MESSAGE_ID_CONFLICT')
        return existing['response']
    subject=validate_subject(message.get('subject') or name)
    targets=[sid for sid,sub in state['subscribers'].items() if (sub.get('subject') or sub['publisher'])==subject]
    def change():
        msg=copy.deepcopy(message)
        msg.update(id=state['next_id'],publisher=name,subject=subject,text=message['payload']['text'],time=message['occurredAt'])
        if subject not in state['topics']: state['topics'].append(subject)
        state['next_id']+=1
        for sid in targets: state['subscribers'][sid]['pending'].append(dict(msg,attempts=0,nextAttemptAt=time.time()+config.DELIVERY_DELAY))
        response=f'OK:PUBLISHED:{mid}:{len(targets)}'
        state['accepted'][mid]=dict(publisher=name,contract=copy.deepcopy(message),response=response)
        record('PUBLISHED',message=msg); return response
    return commit(change)

def health():
    return dict(live=running,ready=running and storage_ok,
                dependencies={'storage':'available' if storage_ok else 'unavailable'})

def handle(role,name,line):
    if role=='PUBLISHER' and line.startswith('PUBLISH:'):
        try: message=json.loads(line[8:])
        except ValueError: raise ContractError('INVALID_JSON')
        return publish(name,message)
    if role=='SUBSCRIBER' and line.startswith('SUBSCRIBE:'):
        return follow_topic(name,line[10:])
    if role=='SUBSCRIBER' and line=='UNSUBSCRIBE':
        def unsubscribe():
            sub=state['subscribers'][name]
            for m in list(sub['pending']): dead_letter(name,m,'UNSUBSCRIBED')
            sub['publisher']=None; sub['subject']=None
        commit(unsubscribe); return 'OK:UNSUBSCRIBED'
    if role=='SUBSCRIBER' and (line.startswith('ACK:') or line.startswith('NACK:')):
        parts=line.split(':',2); pending=state['subscribers'][name]['pending']
        if not pending or pending[0]['messageId']!=parts[1]: return 'OK:STALE_CONFIRMATION'
        m=pending[0]
        if m['attempts']==0: raise ContractError('NOT_DELIVERED')
        if parts[0]=='ACK':
            def acknowledge(): pending.pop(0); record('ACK',name,m)
            commit(acknowledge)
        else:
            code=parts[2] if len(parts)>2 else 'PERMANENT'
            def reject():
                record('NACK',name,m,'TRANSIENT' if code=='TRANSIENT' else 'PERMANENT')
                if code!='TRANSIENT' or m['attempts']>=MAX_ATTEMPTS: dead_letter(name,m,'PROCESSING_FAILED')
            commit(reject)
        return 'OK:'+parts[0]
    if line=='STATUS':
        if role=='SUBSCRIBER':
            s=state['subscribers'][name]; return f"STATUS:publisher={s['publisher']};pending={len(s['pending'])}"
        return 'STATUS:publisher=online'
    raise ContractError('UNKNOWN_COMMAND')

sessions={}
MAX_SESSIONS=16

class Sink:
    def __init__(self): self.events=queue.Queue(maxsize=1)
    def deliver(self,message):
        event=pb.Delivery(message=config.to_proto(message),publisher=message['publisher'],
                          sequence=message['id'],attempt=message['attempts'],historical=message.get('historical',False))
        try: self.events.put_nowait(event)
        except queue.Full: raise OSError('Slow consumer')

def drop_session(token):
    session=sessions.pop(token,None)
    if not session: return
    peers=online_pubs if session['role']=='PUBLISHER' else online_subs
    if peers.get(session['name']) is session['sink']: peers.pop(session['name'],None)

def lookup(token,role=None):
    session=sessions.get(token)
    if not session: raise ContractError('SESSION_NOT_FOUND')
    if role and session['role']!=role: raise ContractError('WRONG_ROLE')
    return session

def invoke(context,operation):
    with lock:
        try: return operation()
        except (ContractError,ValueError) as ex:
            code=str(ex)
            if code=='SESSION_NOT_FOUND': status=grpc.StatusCode.UNAUTHENTICATED
            elif code=='WRONG_ROLE': status=grpc.StatusCode.PERMISSION_DENIED
            elif code in ('ALREADY_SUBSCRIBED','NOT_DELIVERED'): status=grpc.StatusCode.FAILED_PRECONDITION
            elif code in ('ID_ALREADY_CONNECTED','MESSAGE_ID_CONFLICT'): status=grpc.StatusCode.ALREADY_EXISTS
            elif code=='SESSION_LIMIT': status=grpc.StatusCode.RESOURCE_EXHAUSTED
            else: status=grpc.StatusCode.INVALID_ARGUMENT
            context.abort(status,code)
        except OSError: context.abort(grpc.StatusCode.UNAVAILABLE,'STORAGE_UNAVAILABLE')

def follow_topic(name,subject,load_history=False):
    subject=validate_subject(subject); sub=state['subscribers'][name]
    current=sub.get('subject') or sub['publisher']
    if current and current!=subject: raise ContractError('ALREADY_SUBSCRIBED')
    def change():
        sub['publisher']=subject; sub['subject']=subject
        if subject not in state['topics']: state['topics'].append(subject)
        if load_history:
            existing={m['messageId']:m for m in sub['pending']}
            history=[]; seen=set()
            for row in state['history']:
                message=row.get('message') or {}
                if row['event']!='PUBLISHED' or (message.get('subject') or message.get('publisher'))!=subject: continue
                mid=message['messageId']
                if mid in seen: continue
                seen.add(mid)
                queued=existing.get(mid) or dict(copy.deepcopy(message),attempts=0,nextAttemptAt=time.time()+config.DELIVERY_DELAY)
                queued['historical']=True; queued.setdefault('subject',subject); history.append(queued)
            sub['pending']=history+[m for m in sub['pending'] if m['messageId'] not in seen]
        record('SUBSCRIBED',name,detail=subject+('; history' if load_history else ''))
    commit(change)
    return 'OK:SUBSCRIBED:'+subject

class BrokerService(rpc.MessageBrokerServicer):
    def PublishMessage(self,request,context): return self.Publish(request,context)

    def ListTopics(self,request,context):
        with lock: return pb.TopicsReply(subjects=state['topics'])

    def Subscribe(self,request,context):
        def operation():
            session=lookup(request.session_id,'SUBSCRIBER')
            follow_topic(session['name'],request.subject,request.load_history)
        invoke(context,operation)
        yield from self.Watch(pb.Session(session_id=request.session_id),context)

    def Register(self,request,context):
        def operation():
            if request.role not in (pb.PUBLISHER,pb.SUBSCRIBER): raise ContractError('INVALID_ROLE')
            name=request.name
            if not name.strip() or len(name)>100 or any(c in name for c in ':\n\r'): raise ContractError('INVALID_NAME')
            role='PUBLISHER' if request.role==pb.PUBLISHER else 'SUBSCRIBER'
            if any(s['role']==role and s['name']==name for s in sessions.values()): raise ContractError('ID_ALREADY_CONNECTED')
            if len(sessions)>=MAX_SESSIONS: raise ContractError('SESSION_LIMIT')
            def register():
                if role=='PUBLISHER':
                    if name not in state['publishers']: state['publishers'].append(name)
                else: state['subscribers'].setdefault(name,dict(publisher=None,pending=[]))
            commit(register)
            token=str(uuid.uuid4()); sink=Sink()
            sessions[token]=dict(role=role,name=name,sink=sink,watching=False,created=time.monotonic())
            return pb.RegisterReply(session_id=token)
        return invoke(context,operation)

    def Watch(self,request,context):
        def attach():
            session=lookup(request.session_id)
            if session['watching']: raise ContractError('ID_ALREADY_CONNECTED')
            session['watching']=True
            peers=online_pubs if session['role']=='PUBLISHER' else online_subs
            peers[session['name']]=session['sink']
            return session
        session=invoke(context,attach)
        try:
            yield pb.Delivery(heartbeat=True)
            heartbeat=time.monotonic()
            while context.is_active():
                with lock:
                    if sessions.get(request.session_id) is not session: break
                try: yield session['sink'].events.get(timeout=.2)
                except queue.Empty:
                    if time.monotonic()-heartbeat>=2:
                        yield pb.Delivery(heartbeat=True); heartbeat=time.monotonic()
        finally:
            with lock: drop_session(request.session_id)

    def Publish(self,request,context):
        def operation():
            session=lookup(request.session_id,'PUBLISHER')
            message=config.from_proto(request.message)
            answer=publish(session['name'],message).split(':')
            return pb.PublishReply(message_id=answer[2],recipients=int(answer[3]))
        return invoke(context,operation)

    def Follow(self,request,context):
        def operation():
            session=lookup(request.session_id,'SUBSCRIBER')
            return pb.Reply(detail=follow_topic(session['name'],request.subject or request.publisher,request.load_history))
        return invoke(context,operation)

    def Unsubscribe(self,request,context):
        def operation():
            session=lookup(request.session_id,'SUBSCRIBER')
            return pb.Reply(detail=handle('SUBSCRIBER',session['name'],'UNSUBSCRIBE'))
        return invoke(context,operation)

    def Ack(self,request,context): return self.confirm(request,context,False)
    def Nack(self,request,context): return self.confirm(request,context,True)
    def confirm(self,request,context,nack):
        def operation():
            session=lookup(request.session_id,'SUBSCRIBER')
            command=('NACK:' if nack else 'ACK:')+request.message_id
            if nack: command+=':TRANSIENT' if request.transient else ':PERMANENT'
            return pb.Reply(detail=handle('SUBSCRIBER',session['name'],command))
        return invoke(context,operation)

    def Status(self,request,context):
        def operation():
            session=lookup(request.session_id)
            sub=state['subscribers'].get(session['name'],{}) if session['role']=='SUBSCRIBER' else {}
            return pb.StatusReply(publisher=sub.get('publisher') or '',subject=sub.get('subject') or sub.get('publisher') or '',pending=len(sub.get('pending',[])),online=True)
        return invoke(context,operation)

    def Disconnect(self,request,context):
        with lock: drop_session(request.session_id)
        return pb.Reply(detail='OK:DISCONNECTED')

    def Health(self,request,context):
        with lock:
            status=health()
            return pb.HealthReply(live=status['live'],ready=status['ready'],storage=status['dependencies']['storage'])

def maintenance(stop):
    while not stop.wait(.1):
        with lock:
            for token,session in list(sessions.items()):
                if not session['watching'] and time.monotonic()-session['created']>config.IO_TIMEOUT*2:
                    drop_session(token)
            subscribers=list(online_subs)
        for sid in subscribers:
            try: flush(sid)
            except OSError: pass

def main():
    global running
    parser=argparse.ArgumentParser(); parser.add_argument('--port',type=int,default=DEFAULT_PORT); args=parser.parse_args()
    if not 1<=args.port<=65535: parser.error('Port invalid')
    # Bind before loading state so a duplicate launch cannot overwrite an active broker's state.
    server=grpc.server(futures.ThreadPoolExecutor(max_workers=MAX_SESSIONS+8),
                       maximum_concurrent_rpcs=MAX_SESSIONS+8,
                       options=[('grpc.max_receive_message_length',1048576),('grpc.so_reuseport',0)])
    rpc.add_MessageBrokerServicer_to_server(BrokerService(),server)
    if server.add_insecure_port(f'{HOST}:{args.port}')==0: raise OSError('Portul brokerului este ocupat')
    load(); server.start(); running=True
    stop=threading.Event(); worker=threading.Thread(target=maintenance,args=(stop,),daemon=True); worker.start()
    log('GRPC_STARTED')
    try: server.wait_for_termination()
    except KeyboardInterrupt: pass
    finally:
        running=False; stop.set()
        with lock:
            for token in list(sessions): drop_session(token)
        server.stop(1).wait(); worker.join(2)

if __name__=='__main__': main()
