"""Shared configuration, contracts, logging, health and TCP client implementation."""

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
PORT = int(os.getenv('BROKER_PORT', '8080'))
STATE_FILE = path('BROKER_STATE_FILE', 'broker_state.json')
INBOX_FILE = path('CONSUMER_INBOX_FILE', 'consumer_inbox.sqlite3')
LOG_FILE = path('BROKER_LOG_FILE', 'broker.log.jsonl')
MAX_ATTEMPTS = int(os.getenv('MAX_ATTEMPTS', '3'))
ACK_TIMEOUT = float(os.getenv('ACK_TIMEOUT', '5'))
DELIVERY_DELAY = float(os.getenv('DELIVERY_DELAY', '3'))
BACKOFF_BASE = float(os.getenv('BACKOFF_BASE', '1'))
BACKOFF_CAP = float(os.getenv('BACKOFF_CAP', '15'))
JITTER = float(os.getenv('RETRY_JITTER', '0.25'))
IO_TIMEOUT = float(os.getenv('IO_TIMEOUT', '3'))
if not (1 <= PORT <= 65535 and MAX_ATTEMPTS >= 1 and ACK_TIMEOUT > 0
        and 0 <= DELIVERY_DELAY < float('inf')
        and 0 <= BACKOFF_BASE <= BACKOFF_CAP and 0 <= JITTER <= 1 and IO_TIMEOUT > 0):
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
    return message

def create(text, correlation_id=None, message_id=None):
    return validate(dict(messageId=message_id or str(uuid.uuid4()),
                         correlationId=correlation_id or str(uuid.uuid4()),
                         messageType='TextPublished', schemaVersion=1,
                         occurredAt=utcnow(), payload={'text': text}))


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


# Read-only health check

"""Read-only TCP health endpoint client; exit 0 when ready, 1 otherwise."""
import argparse
import json
import socket

def check(host=HOST,port=PORT):
    with socket.create_connection((host,port),timeout=IO_TIMEOUT) as sock:
        sock.sendall(b'HEALTH\n')
        with sock.makefile('r',encoding='utf-8') as reader: response=reader.readline(65537)
    if not response.startswith('HEALTH:'): raise ValueError('Invalid health response')
    return json.loads(response[7:])

if __name__=='__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('--host',default=HOST)
    parser.add_argument('--port',type=int,default=PORT); args=parser.parse_args()
    try: result=check(args.host,args.port)
    except (OSError,ValueError): result={'live':False,'ready':False,'dependencies':{'storage':'unknown'}}
    print(json.dumps(result)); raise SystemExit(0 if result['ready'] else 1)


# Common client transport, inbox and outbox

import json
import socket
import threading
import sqlite3
from pathlib import Path
from contextlib import closing
import os
import time
import random


def store_message(subscriber, message):
    validate(message)
    with closing(sqlite3.connect(INBOX_FILE,timeout=10)) as db, db:
        db.execute('CREATE TABLE IF NOT EXISTS inbox_v1 (subscriber TEXT, message_id TEXT, payload TEXT NOT NULL, PRIMARY KEY(subscriber,message_id))')
        result=db.execute('INSERT OR IGNORE INTO inbox_v1 VALUES (?,?,?)',(subscriber,message['messageId'],json.dumps(message,ensure_ascii=False)))
        return result.rowcount==1

def outbox_db():
    db=sqlite3.connect(INBOX_FILE,timeout=10)
    db.execute('CREATE TABLE IF NOT EXISTS outbox_v1 (publisher TEXT, message_id TEXT, payload TEXT, attempts INTEGER, due REAL, status TEXT, PRIMARY KEY(publisher,message_id))')
    db.commit()
    return db

def next_local_port(start, excluded=(), limit=128):
    """Suggest an available source port; never reserve it or bypass OS restrictions."""
    blocked=set(int(port) for port in excluded)
    for port in range(max(1,int(start)),min(65536,max(1,int(start))+limit)):
        if port in blocked: continue
        try:
            with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as probe:
                probe.bind(('',port))
            return port
        except OSError: continue
    return 0  # Automatic assignment if there is no free nearby candidate.

def connection_error_text(error,local_port):
    code=getattr(error,'winerror',None)
    if code in (10013,10048) and int(local_port or 0):
        return (f'Portul local {local_port} este indisponibil: poate fi rezervat de Windows, '
                'ocupat sau in curs de eliberare. Alege alt port local sau 0 pentru alegere automata. '
                'Portul brokerului ramane neschimbat.')
    return str(error)

class TCPClient:
    def __init__(self, app, role, name, host, port, publisher='', local_port=0):
        self.app=app; self.role=role; self.name=name; self.publisher=publisher
        local_port=int(local_port or 0)
        if not 0<=local_port<=65535: raise ValueError('Portul local trebuie sa fie intre 0 si 65535')
        source_address=('',local_port) if local_port else None
        self.sock=socket.create_connection((host,int(port)),timeout=4,source_address=source_address)
        self.local_address=self.sock.getsockname()
        self.broker_address=self.sock.getpeername()
        self.sock.settimeout(4); self.lock=threading.Lock(); self.active=True
        self.send(role+':'+name)
        # Verify handshake before showing ONLINE. Read exactly through the newline.
        answer=bytearray()
        while not answer.endswith(b'\n'):
            part=self.sock.recv(1)
            if not part: self.sock.close(); raise OSError('Brokerul a inchis conexiunea')
            answer.extend(part)
            if len(answer)>4096: self.sock.close(); raise OSError('Raspuns broker invalid')
        if answer.decode().strip()!='OK:CONNECTED':
            self.sock.close(); raise OSError(answer.decode().strip())
        self.sock.settimeout(None)
        self.reader_thread=threading.Thread(target=self.read,daemon=True); self.reader_thread.start()
        if role=='PUBLISHER':
            self.publisher_thread=threading.Thread(target=self.retry_publications,daemon=True); self.publisher_thread.start()
        if role=='SUBSCRIBER' and publisher: self.send('SUBSCRIBE:'+publisher)
    def send(self, line):
        if self.role=='PUBLISHER' and line.startswith('PUBLISH:'):
            message=create(line[8:])
            with closing(outbox_db()) as db, db:
                db.execute('INSERT INTO outbox_v1 VALUES (?,?,?,0,0,?)',
                           (self.name,message['messageId'],json.dumps(message,ensure_ascii=False),'pending'))
            emit('publisher','REQUEST_SAVED',message)
            return
        with self.lock: self.sock.sendall((line+'\n').encode('utf-8'))
    def retry_publications(self):
        while self.active:
            try:
                with closing(outbox_db()) as db, db:
                    row=db.execute("SELECT message_id,payload,attempts,due FROM outbox_v1 WHERE publisher=? AND status='pending' ORDER BY rowid LIMIT 1",(self.name,)).fetchone()
                    if row and time.time()>=row[3]:
                        mid,payload,attempts,_=row
                        if attempts>=MAX_ATTEMPTS:
                            db.execute("UPDATE outbox_v1 SET status='failed' WHERE publisher=? AND message_id=?",(self.name,mid))
                            self.app.events.put(('log',self.role,self.name,'Publicare neconfirmata dupa limita de incercari: '+mid))
                            row=None
                        else:
                            base=min(BACKOFF_CAP,BACKOFF_BASE*2**attempts)
                            due=time.time()+ACK_TIMEOUT+min(BACKOFF_CAP,base*random.uniform(1-JITTER,1+JITTER))
                            db.execute('UPDATE outbox_v1 SET attempts=?,due=? WHERE publisher=? AND message_id=?',(attempts+1,due,self.name,mid))
                    else: row=None
                if row:
                    with self.lock: self.sock.sendall(('PUBLISH:'+payload+'\n').encode('utf-8'))
                    emit('publisher','PUBLISH_ATTEMPT',json.loads(payload))
            except (OSError,sqlite3.Error) as ex:
                self.app.events.put(('log',self.role,self.name,'Publicarea ramane in outbox; verifica conexiunea si stocarea.'))
                emit('publisher','PUBLISH_FAILED',result='pending',error='DEPENDENCY_UNAVAILABLE')
                self.close()
            time.sleep(.1)
    def read(self):
        try:
            with self.sock.makefile('r',encoding='utf-8') as f:
                for raw in f:
                    line=raw.strip()
                    if line.startswith('MESSAGE:') and self.role=='SUBSCRIBER':
                        m=None
                        try:
                            started=time.monotonic()
                            m=json.loads(line[8:])
                            fresh=store_message(self.name,m)
                            # Explicit diagnostic mode for the required real crash scenario.
                            if fresh and os.getenv('CONSUMER_CRASH_AFTER_COMMIT')=='1': os._exit(86)
                            if fresh: self.app.events.put(('log',self.role,self.name,f"MESAJ #{m['id']} de la {m['publisher']}: {m['text']}"))
                            emit('subscriber','EFFECT_COMMITTED' if fresh else 'DUPLICATE_SKIPPED',m,started=started)
                            self.send('ACK:'+m['messageId'])
                        except Exception as ex:
                            self.app.events.put(('log',self.role,self.name,'Procesare esuata: '+str(ex)))
                            if isinstance(m,dict) and isinstance(m.get('messageId'),str):
                                code='TRANSIENT' if isinstance(ex,(sqlite3.OperationalError,OSError)) else 'PERMANENT'
                                self.send('NACK:'+m['messageId']+':'+code)
                    else:
                        if self.role=='PUBLISHER' and line.startswith('OK:PUBLISHED:'):
                            mid=line.split(':')[2]
                            with closing(outbox_db()) as db, db:
                                db.execute("UPDATE outbox_v1 SET status='accepted' WHERE publisher=? AND message_id=?",(self.name,mid))
                        self.app.events.put(('log',self.role,self.name,line))
        except OSError: pass
        finally:
            self.active=False; self.app.events.put(('offline',self.role,self.name,''))
    def close(self):
        self.active=False
        try: self.sock.shutdown(socket.SHUT_RDWR)
        except OSError: pass
        self.sock.close()

