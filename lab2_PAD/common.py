"""Shared configuration, contracts, logging, health and gRPC client implementation."""

# Configuration

"""Environment overrides .env; paths are relative to this project."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
env_file = ROOT / '.env'
if env_file.exists():
    for raw in env_file.read_text(encoding='utf-8-sig').splitlines():
        raw = raw.strip()
        if raw and not raw.startswith('#') and '=' in raw:
            key, value = raw.split('=', 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

def path(name, default):
    value = Path(os.getenv(name, default))
    return value if value.is_absolute() else ROOT / value

HOST = os.getenv('BROKER_HOST', '127.0.0.1')
PORT = int(os.getenv('BROKER_PORT', '50051'))
TOPICS = tuple(t.strip() for t in os.getenv('BROKER_TOPICS','Prestatii artistice|Concursuri de recital|Competitii Sportive').split('|') if t.strip())
if not TOPICS: raise ValueError('Configureaza cel putin un topic')
STATE_FILE = path('BROKER_STATE_FILE', 'broker_state.json')
INBOX_FILE = path('CONSUMER_INBOX_FILE', 'consumer_inbox.sqlite3')
LOG_FILE = path('BROKER_LOG_FILE', 'broker.log.jsonl')
MAX_ATTEMPTS = int(os.getenv('MAX_ATTEMPTS', '3'))
ACK_TIMEOUT = float(os.getenv('ACK_TIMEOUT', '5'))
DELIVERY_DELAY = float(os.getenv('DELIVERY_DELAY', '2'))
BACKOFF_BASE = float(os.getenv('BACKOFF_BASE', '1'))
BACKOFF_CAP = float(os.getenv('BACKOFF_CAP', '15'))
JITTER = float(os.getenv('RETRY_JITTER', '0.25'))
IO_TIMEOUT = float(os.getenv('IO_TIMEOUT', '3'))
STREAM_TIMEOUT = float(os.getenv('GRPC_STREAM_TIMEOUT', '3600'))
if not (1 <= PORT <= 65535 and MAX_ATTEMPTS >= 1 and ACK_TIMEOUT > 0
        and 0 <= DELIVERY_DELAY < float('inf')
        and 0 <= BACKOFF_BASE <= BACKOFF_CAP and 0 <= JITTER <= 1 and IO_TIMEOUT > 0
        and 0 < STREAM_TIMEOUT < float('inf')):
    raise ValueError('Configuratie invalida; verifica .env.example')


# Message contracts

"""Version 1 message contract; unknown additive fields are allowed."""
import uuid
from datetime import datetime, timezone

def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')

class ContractError(ValueError):
    pass

def validate(message):
    if not isinstance(message, dict):
        raise ContractError('INVALID_CONTRACT: expected object')
    for field in ('messageId', 'correlationId'):
        value = message.get(field)
        if not isinstance(value, str):
            raise ContractError('INVALID_CONTRACT: ' + field)
        try: uuid.UUID(value)
        except ValueError: raise ContractError('INVALID_CONTRACT: ' + field)
    if type(message.get('schemaVersion')) is not int or message['schemaVersion'] != 1:
        raise ContractError('UNSUPPORTED_VERSION')
    if message.get('messageType') != 'TextPublished':
        raise ContractError('UNSUPPORTED_TYPE')
    try:
        stamp = datetime.fromisoformat(message['occurredAt'].replace('Z', '+00:00'))
        if stamp.utcoffset() is None or stamp.utcoffset().total_seconds() != 0:
            raise ValueError()
    except (KeyError, AttributeError, TypeError, ValueError):
        raise ContractError('INVALID_CONTRACT: occurredAt must be UTC')
    payload = message.get('payload')
    if not isinstance(payload, dict) or not isinstance(payload.get('text'), str) or not payload['text'].strip():
        raise ContractError('INVALID_CONTRACT: payload.text')
    if len(payload['text']) > 10000:
        raise ContractError('PAYLOAD_TOO_LARGE')
    if 'subject' in message: validate_subject(message['subject'])
    return message

def validate_subject(subject):
    if not isinstance(subject,str) or not subject.strip() or len(subject)>100 or any(c in subject for c in ':\n\r'):
        raise ContractError('INVALID_SUBJECT')
    return subject.strip()

def create(text, correlation_id=None, message_id=None, subject=None):
    message=dict(messageId=message_id or str(uuid.uuid4()),
                         correlationId=correlation_id or str(uuid.uuid4()),
                         messageType='TextPublished', schemaVersion=1,
                         occurredAt=utcnow(), payload={'text': text})
    if subject is not None: message['subject']=validate_subject(subject)
    return validate(message)


# Structured logging

import json
import time

def emit(component,event,message=None,result='ok',error=None,started=None):
    m=message or {}
    row=dict(timestamp=utcnow(),level='ERROR' if error else 'INFO',component=component,
             event=event,correlationId=m.get('correlationId'),messageId=m.get('messageId'),
             result=result,durationMs=round((time.monotonic()-started)*1000,3) if started else 0,error=error)
    try:
        with open(LOG_FILE,'a',encoding='utf-8') as out: out.write(json.dumps(row)+'\n')
    except OSError: pass



import threading
import queue
import random
import sqlite3
from contextlib import closing
import grpc
from google.protobuf.json_format import MessageToDict
import broker_pb2 as pb
import broker_pb2_grpc as rpc

CONTRACT_KEYS={'messageId','correlationId','messageType','schemaVersion','occurredAt','payload','subject'}
BROKER_KEYS={'id','publisher','text','time','attempts','nextAttemptAt','historical'}

def to_proto(message):
    validate(message)
    result=pb.Message(message_id=message['messageId'],correlation_id=message['correlationId'],
                      message_type=message['messageType'],schema_version=message['schemaVersion'],
                      occurred_at=message['occurredAt'],subject=message.get('subject',''),payload=pb.TextPayload(text=message['payload']['text']))
    result.extensions.update({k:v for k,v in message.items() if k not in CONTRACT_KEYS|BROKER_KEYS})
    result.payload.extensions.update({k:v for k,v in message['payload'].items() if k!='text'})
    return result

def from_proto(message):
    data=MessageToDict(message.extensions)
    payload=MessageToDict(message.payload.extensions)
    payload['text']=message.payload.text
    data.update(messageId=message.message_id,correlationId=message.correlation_id,
                messageType=message.message_type,schemaVersion=message.schema_version,
                occurredAt=message.occurred_at,payload=payload)
    if message.subject: data['subject']=message.subject
    return validate(data)

def connect(host=HOST,port=PORT):
    return grpc.insecure_channel(f'{host}:{int(port)}',options=[
        ('grpc.use_local_subchannel_pool',1),('grpc.max_receive_message_length',1048576)])

def check(host=HOST,port=PORT):
    try:
        with connect(host,port) as channel:
            answer=rpc.MessageBrokerStub(channel).Health(pb.Empty(),timeout=IO_TIMEOUT)
            return dict(live=answer.live,ready=answer.ready,dependencies={'storage':answer.storage})
    except grpc.RpcError as ex: raise OSError('Brokerul gRPC nu raspunde: '+ex.code().name) from ex

def store_message(subscriber,message):
    validate(message)
    with closing(sqlite3.connect(INBOX_FILE,timeout=10)) as db,db:
        db.execute('CREATE TABLE IF NOT EXISTS inbox_v1 (subscriber TEXT, message_id TEXT, payload TEXT NOT NULL, PRIMARY KEY(subscriber,message_id))')
        cursor=db.execute('INSERT OR IGNORE INTO inbox_v1 VALUES (?,?,?)',(subscriber,message['messageId'],json.dumps(message,ensure_ascii=False)))
        return cursor.rowcount==1

def outbox_db():
    db=sqlite3.connect(INBOX_FILE,timeout=10)
    db.execute('CREATE TABLE IF NOT EXISTS outbox_v1 (publisher TEXT, message_id TEXT, payload TEXT, attempts INTEGER, due REAL, status TEXT, PRIMARY KEY(publisher,message_id))')
    db.commit()
    return db

class GRPCClient:
    def __init__(self,app,role,name,host,port,publisher='',subject=None,load_history=False):
        self.app=app; self.role=role; self.name=name; self.active=False
        self.channel=connect(host,port); self.stub=rpc.MessageBrokerStub(self.channel)
        self.subject=subject if subject is not None else publisher
        self.session=None; self.stream=None; self.stop_event=threading.Event()
        try:
            registered=self.stub.Register(pb.RegisterRequest(role=pb.PUBLISHER if role=='PUBLISHER' else pb.SUBSCRIBER,name=name),timeout=IO_TIMEOUT)
            self.session=registered.session_id
            if role=='SUBSCRIBER' and self.subject:
                self.stream=self.stub.Subscribe(pb.SubscribeRequest(session_id=self.session,subject=self.subject,load_history=load_history),timeout=STREAM_TIMEOUT)
            else: self.stream=self.stub.Watch(pb.Session(session_id=self.session),timeout=STREAM_TIMEOUT)
            first=next(self.stream)
            if not first.heartbeat: raise OSError('Invalid gRPC stream handshake')
            self.active=True
        except Exception:
            self.close(); raise
        self.reader_thread=threading.Thread(target=self.read,daemon=True); self.reader_thread.start()
        if role=='PUBLISHER':
            self.publisher_thread=threading.Thread(target=self.retry_publications,daemon=True); self.publisher_thread.start()

    def event(self,text): self.app.events.put(('log',self.role,self.name,text))

    def send(self,line):
        if not self.active: raise OSError('Clientul gRPC este offline')
        if self.role=='PUBLISHER' and line.startswith('PUBLISH:'):
            message=create(line[8:],subject=self.subject or TOPICS[0])
            with closing(outbox_db()) as db,db:
                db.execute('INSERT INTO outbox_v1 VALUES (?,?,?,0,0,?)',
                           (self.name,message['messageId'],json.dumps(message,ensure_ascii=False),'pending'))
            emit('publisher','REQUEST_SAVED',message); return
        try:
            if line.startswith('SUBSCRIBE:'):
                answer=self.stub.Follow(pb.FollowRequest(session_id=self.session,subject=line[10:]),timeout=IO_TIMEOUT)
                self.subject=line[10:]; self.event(answer.detail)
            elif line=='UNSUBSCRIBE': self.event(self.stub.Unsubscribe(pb.Session(session_id=self.session),timeout=IO_TIMEOUT).detail)
            elif line=='STATUS':
                answer=self.stub.Status(pb.Session(session_id=self.session),timeout=IO_TIMEOUT)
                self.event(f'STATUS: topic={answer.subject}; pending={answer.pending}; online={answer.online}')
            else: raise ValueError('Comanda necunoscuta')
        except grpc.RpcError as ex: raise OSError(ex.code().name+': '+(ex.details() or '')) from ex

    def retry_publications(self):
        while self.active and not self.stop_event.is_set():
            row=None
            try:
                with closing(outbox_db()) as db,db:
                    row=db.execute("SELECT message_id,payload,attempts,due FROM outbox_v1 WHERE publisher=? AND status='pending' ORDER BY rowid LIMIT 1",(self.name,)).fetchone()
                    if row and time.time()>=row[3]:
                        mid,payload,attempts,_=row
                        if attempts>=MAX_ATTEMPTS:
                            db.execute("UPDATE outbox_v1 SET status='failed' WHERE publisher=? AND message_id=?",(self.name,mid))
                            self.event('Publicare neconfirmata dupa limita: '+mid); row=None
                        else:
                            base=min(BACKOFF_CAP,BACKOFF_BASE*2**attempts)
                            delay=ACK_TIMEOUT+min(BACKOFF_CAP,base*random.uniform(1-JITTER,1+JITTER))
                            db.execute('UPDATE outbox_v1 SET attempts=?,due=? WHERE publisher=? AND message_id=?',(attempts+1,time.time()+delay,self.name,mid))
                    else: row=None
                if row:
                    message=json.loads(payload); emit('publisher','PUBLISH_ATTEMPT',message)
                    answer=self.stub.PublishMessage(pb.PublishRequest(session_id=self.session,message=to_proto(message)),timeout=IO_TIMEOUT)
                    with closing(outbox_db()) as db,db:
                        db.execute("UPDATE outbox_v1 SET status='accepted' WHERE publisher=? AND message_id=?",(self.name,mid))
                    self.event(f'OK:PUBLISHED:{answer.message_id}:{answer.recipients}')
            except grpc.RpcError as ex:
                self.event('Publicare gRPC: '+ex.code().name)
                if row and ex.code() not in (grpc.StatusCode.UNAVAILABLE,grpc.StatusCode.DEADLINE_EXCEEDED,grpc.StatusCode.RESOURCE_EXHAUSTED):
                    with closing(outbox_db()) as db,db:
                        db.execute("UPDATE outbox_v1 SET status='failed' WHERE publisher=? AND message_id=?",(self.name,mid))
            except (OSError,ValueError,sqlite3.Error):
                self.event('Publicare esuata; verifica stocarea si contractul')
                self.stop_event.set(); self.active=False
            self.stop_event.wait(.1)

    def read(self):
        history_shown=set()
        try:
            for delivery in self.stream:
                if delivery.heartbeat: continue
                message=None
                try:
                    started=time.monotonic(); message=from_proto(delivery.message)
                    message.update(id=delivery.sequence,publisher=delivery.publisher,text=message['payload']['text'],
                                   time=message['occurredAt'],attempts=delivery.attempt)
                    fresh=store_message(self.name,message)
                    if fresh and os.getenv('CONSUMER_CRASH_AFTER_COMMIT')=='1': os._exit(86)
                    emit('subscriber','EFFECT_COMMITTED' if fresh else 'DUPLICATE_SKIPPED',message,started=started)
                    if fresh or (delivery.historical and message['messageId'] not in history_shown):
                        if delivery.historical: history_shown.add(message['messageId'])
                        self.event(f"{'ISTORIC' if delivery.historical else 'MESAJ'} #{delivery.sequence} [{message.get('subject',delivery.publisher)}] de la {delivery.publisher}: {message['text']}")
                    self.stub.Ack(pb.Confirmation(session_id=self.session,message_id=message['messageId']),timeout=IO_TIMEOUT)
                    self.event('OK:ACK')
                except grpc.RpcError as ex:
                    self.event('Confirmare gRPC esuata: '+ex.code().name)
                except (ValueError,OSError,sqlite3.Error) as ex:
                    self.event('Procesare esuata; mesajul nu este confirmat')
                    mid=delivery.message.message_id
                    if mid:
                        try:
                            self.stub.Nack(pb.Confirmation(session_id=self.session,message_id=mid,
                                transient=isinstance(ex,(OSError,sqlite3.OperationalError))),timeout=IO_TIMEOUT)
                        except grpc.RpcError: pass
        except grpc.RpcError as ex:
            if not self.stop_event.is_set(): self.event('Fluxul gRPC s-a inchis: '+ex.code().name)
        finally:
            self.active=False; self.stop_event.set()
            self.app.events.put(('offline',self.role,self.name,''))

    def close(self):
        self.active=False; self.stop_event.set()
        if self.session:
            try: self.stub.Disconnect(pb.Session(session_id=self.session),timeout=IO_TIMEOUT)
            except grpc.RpcError: pass
            self.session=None
        if self.stream: self.stream.cancel()
        self.channel.close()

if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(); parser.add_argument('--port',type=int,default=PORT); args=parser.parse_args()
    try: result=check(port=args.port)
    except OSError: result={'live':False,'ready':False,'dependencies':{'storage':'unknown'}}
    print(json.dumps(result)); raise SystemExit(0 if result['ready'] else 1)
