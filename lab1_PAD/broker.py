"""Persistent, bounded delivery with one outstanding message per subscriber."""
import argparse
import copy
import json
import os
import random
import socket
import threading
import time
import uuid
from pathlib import Path
import common as config
from common import ContractError, create, utcnow, validate

HOST=config.HOST; DEFAULT_PORT=config.PORT; STATE_FILE=config.STATE_FILE
MAX_ATTEMPTS=config.MAX_ATTEMPTS; ACK_TIMEOUT=config.ACK_TIMEOUT
lock=threading.RLock(); online_subs={}; online_pubs={}; running=False; storage_ok=True

def empty_state():
    return dict(next_id=1,publishers=[],subscribers={},history=[],dlq=[],accepted={})

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

def send_line(conn,text): conn.sendall((text+'\n').encode('utf-8'))

class Frames:
    def __init__(self,conn): self.conn=conn; self.buffer=bytearray()
    def readline(self,handshake=False):
        started=time.monotonic()
        while b'\n' not in self.buffer:
            try: chunk=self.conn.recv(4096)
            except socket.timeout:
                if handshake or (self.buffer and time.monotonic()-started>60): raise
                continue
            if not chunk: return ''
            self.buffer.extend(chunk)
            if len(self.buffer)>65536 and b'\n' not in self.buffer: raise OSError('FRAME_TOO_LARGE')
        line,_,remaining=self.buffer.partition(b'\n'); self.buffer=bytearray(remaining)
        if len(line)>65536: raise OSError('FRAME_TOO_LARGE')
        return line.decode('utf-8')+'\n'

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
        try: send_line(conn,'MESSAGE:'+json.dumps(m,ensure_ascii=False))
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
    targets=[sid for sid,sub in state['subscribers'].items() if sub['publisher']==name]
    def change():
        msg=copy.deepcopy(message)
        msg.update(id=state['next_id'],publisher=name,text=message['payload']['text'],time=message['occurredAt'])
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
        pub=line[10:].strip(); sub=state['subscribers'][name]
        if not pub or len(pub)>100 or ':' in pub: raise ContractError('INVALID_PUBLISHER')
        if sub['publisher'] and sub['publisher']!=pub: raise ContractError('ALREADY_SUBSCRIBED')
        commit(lambda:sub.update(publisher=pub)); return 'OK:SUBSCRIBED:'+pub
    if role=='SUBSCRIBER' and line=='UNSUBSCRIBE':
        def unsubscribe():
            sub=state['subscribers'][name]
            for m in list(sub['pending']): dead_letter(name,m,'UNSUBSCRIBED')
            sub['publisher']=None
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

def client(conn,addr):
    role=name=None; registered=False
    conn.settimeout(config.IO_TIMEOUT); f=Frames(conn)
    try:
        hello=f.readline(handshake=True).strip()
        if hello=='HEALTH':
            with lock: send_line(conn,'HEALTH:'+json.dumps(health()))
            return
        if ':' not in hello: return
        role,name=hello.split(':',1)
        if role not in ('PUBLISHER','SUBSCRIBER') or not name or len(name)>100 or ':' in name: return
        with lock:
            peers=online_pubs if role=='PUBLISHER' else online_subs
            if name in peers: send_line(conn,'ERROR:ID_ALREADY_CONNECTED'); return
            def register():
                if role=='PUBLISHER':
                    if name not in state['publishers']: state['publishers'].append(name)
                else: state['subscribers'].setdefault(name,dict(publisher=None,pending=[]))
            commit(register); peers[name]=conn; registered=True; send_line(conn,'OK:CONNECTED')
        while True:
            raw=f.readline()
            if not raw: break
            if len(raw)>65536: send_line(conn,'ERROR:FRAME_TOO_LARGE'); break
            with lock:
                try: send_line(conn,handle(role,name,raw.strip()))
                except (ContractError,ValueError) as ex: send_line(conn,'ERROR:'+str(ex))
                except OSError: send_line(conn,'ERROR:STORAGE_UNAVAILABLE')
    except (OSError,UnicodeError): log('CONNECTION_CLOSED',result='closed')
    finally:
        if registered:
            with lock:
                peers=online_pubs if role=='PUBLISHER' else online_subs
                if peers.get(name) is conn: peers.pop(name,None)
        conn.close()

def main():
    global running
    parser=argparse.ArgumentParser(); parser.add_argument('--port',type=int,default=DEFAULT_PORT); args=parser.parse_args()
    load()
    server=socket.socket(); server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    server.bind((HOST,args.port)); server.listen(); server.settimeout(0.5); running=True
    stop=threading.Event(); worker=threading.Thread(target=retry_worker,args=(stop,),daemon=True); worker.start()
    log('STARTED')
    try:
        while True:
            try: conn,addr=server.accept()
            except socket.timeout: continue
            threading.Thread(target=client,args=(conn,addr),daemon=True).start()
    except KeyboardInterrupt: pass
    finally: running=False; stop.set(); server.close(); worker.join(2)

if __name__=='__main__': main()
