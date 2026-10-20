import copy
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import queue
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch, MagicMock
from contextlib import closing
import broker
import common as client_core
import common as config
from common import ContractError, create, validate
from common import check
from publisher import publish_request

ROOT=Path(__file__).resolve().parent

class LocalTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir=ROOT)
        self.patches=[patch.object(config,'DELIVERY_DELAY',0),patch.object(broker,'STATE_FILE',Path(self.tmp.name)/'state.json'),
                      patch.object(config,'LOG_FILE',Path(self.tmp.name)/'log.jsonl')]
        for p in self.patches: p.start()
        broker.state=broker.empty_state(); broker.online_subs.clear(); broker.online_pubs.clear()
        broker.storage_ok=True
        broker.state['subscribers']['sub1']=dict(publisher='pub1',pending=[])
    def tearDown(self):
        for p in reversed(self.patches): p.stop()
        self.tmp.cleanup()
    def message(self):
        m=create('test'); broker.publish('pub1',m); return m
    def test_delivery_waits_until_persisted_deadline(self):
        with patch.object(config,'DELIVERY_DELAY',3),patch('broker.time.time',return_value=100):
            message=self.message()
        broker.online_subs['sub1']=object()
        with patch.object(broker,'send_line') as send:
            with patch('broker.time.time',return_value=102.99): broker.flush('sub1')
            send.assert_not_called()
            broker.load()
            pending=broker.state['subscribers']['sub1']['pending'][0]
            self.assertEqual(pending['attempts'],0)
            self.assertEqual(pending['nextAttemptAt'],103)
            with patch('broker.time.time',return_value=103): broker.flush('sub1')
            self.assertEqual(send.call_count,1)
            self.assertIn(message['messageId'],send.call_args.args[1])

    def test_unsubscribe_during_delay_moves_message_to_dlq(self):
        with patch.object(config,'DELIVERY_DELAY',3): message=self.message()
        broker.online_subs['sub1']=object()
        with patch.object(broker,'send_line') as send:
            broker.flush('sub1')
            broker.handle('SUBSCRIBER','sub1','UNSUBSCRIBE')
            send.assert_not_called()
        broker.load()
        self.assertEqual(broker.state['subscribers']['sub1']['pending'],[])
        entry=broker.state['dlq'][0]
        self.assertEqual(entry['message']['messageId'],message['messageId'])
        self.assertEqual(entry['reason'],'UNSUBSCRIBED')
        self.assertEqual(entry['message']['attempts'],0)

    def test_port_suggestion_skips_denied_and_occupied_ports(self):
        attempts=[]
        def fake_socket(*args):
            probe=MagicMock()
            def bind(address):
                attempts.append(address[1])
                if address[1] in (8082,8083): raise OSError('unavailable')
            probe.__enter__.return_value.bind.side_effect=bind
            return probe
        with patch.object(config.socket,'socket',side_effect=fake_socket):
            self.assertEqual(config.next_local_port(8082,excluded=[8084]),8085)
        self.assertEqual(attempts,[8082,8083,8085])
        with patch.object(config.socket,'socket',side_effect=OSError('denied')):
            self.assertEqual(config.next_local_port(65000,limit=2),0)
        self.assertEqual(config.next_local_port(65536),0)

    def test_reserved_port_error_explains_how_to_continue(self):
        error=OSError('denied'); error.winerror=10013
        detail=config.connection_error_text(error,8082)
        self.assertIn('8082',detail); self.assertIn('Windows',detail)
        self.assertIn('0',detail)

    def test_contract_additive_fields_compatible(self):
        m=create('text'); m['futureOptionalField']={'anything':True}
        self.assertIs(validate(m),m)
        fixture=json.loads((ROOT/'contracts'/'example-v1.json').read_text())
        self.assertEqual(validate(fixture)['schemaVersion'],1)
    def test_contract_rejects_breaking_versions_and_bad_utc(self):
        for field,value in [('schemaVersion',2),('occurredAt','2026-10-14T10:00:00'),('messageType','Other')]:
            m=create('x'); m[field]=value
            with self.assertRaises(ContractError): validate(m)
    def test_malformed_contracts(self):
        for m in [None,{},dict(create('x'),messageId='bad'),dict(create('x'),payload={'text':''})]:
            with self.assertRaises(ContractError): validate(m)
    def test_producer_dedup_survives_reload_and_detects_conflict(self):
        m=self.message(); broker.load(); broker.publish('pub1',m)
        self.assertEqual(len(broker.state['subscribers']['sub1']['pending']),1)
        changed=copy.deepcopy(m); changed['payload']['text']='different'
        with self.assertRaises(ContractError): broker.publish('pub1',changed)
    def test_retry_exponential_jitter_bounds(self):
        with patch.object(config,'BACKOFF_BASE',1),patch.object(config,'BACKOFF_CAP',10),patch.object(config,'JITTER',0.25):
            for attempt in range(1,8):
                base=min(10,2**(attempt-1))
                for _ in range(20): self.assertTrue(base*.75<=broker.retry_delay(attempt)<=min(10,base*1.25))
    def test_fifo_retry_limit_dlq_and_ack(self):
        first=self.message(); second=self.message(); sent=[]
        broker.online_subs['sub1']=object()
        with patch.object(broker,'send_line',lambda c,line:sent.append(json.loads(line[8:]))):
            for _ in range(broker.MAX_ATTEMPTS):
                broker.state['subscribers']['sub1']['pending'][0]['nextAttemptAt']=0
                broker.flush('sub1')
            self.assertEqual({m['messageId'] for m in sent},{first['messageId']})
            broker.state['subscribers']['sub1']['pending'][0]['nextAttemptAt']=0; broker.flush('sub1')
            broker.flush('sub1')
            self.assertEqual(sent[-1]['messageId'],second['messageId'])
            broker.handle('SUBSCRIBER','sub1','ACK:'+second['messageId'])
        broker.load()
        self.assertEqual(len(broker.state['dlq']),1)
        self.assertEqual(broker.state['subscribers']['sub1']['pending'],[])
        self.assertIn('ACK',[r['event'] for r in broker.state['history']])
    def test_transient_nack_retries_permanent_nack_dlq(self):
        m=self.message(); pending=broker.state['subscribers']['sub1']['pending']; pending[0]['attempts']=1
        broker.handle('SUBSCRIBER','sub1','NACK:'+m['messageId']+':TRANSIENT')
        self.assertEqual(len(pending),1)
        broker.handle('SUBSCRIBER','sub1','NACK:'+m['messageId']+':PERMANENT')
        self.assertEqual(len(broker.state['dlq']),1)
    def test_storage_failure_rolls_back_and_health_recovers(self):
        before=copy.deepcopy(broker.state)
        with patch('broker.os.replace',side_effect=OSError('simulated')):
            with self.assertRaises(OSError): self.message()
        self.assertEqual(broker.state,before)
        with patch.object(broker,'running',True):
            self.assertFalse(broker.health()['ready']); self.assertTrue(broker.health()['live'])
            broker.save(); self.assertTrue(broker.health()['ready'])
    def test_health_does_not_write(self):
        with patch.object(broker,'save',side_effect=AssertionError('health wrote state')):
            broker.health()
    def test_corrupt_state_is_not_overwritten(self):
        Path(broker.STATE_FILE).write_text('{broken',encoding='utf-8')
        with self.assertRaises(ValueError): broker.load()
        self.assertEqual(Path(broker.STATE_FILE).read_text(),'{broken')
    def test_structured_logs_do_not_expose_payload(self):
        m=create('secret text'); broker.publish('pub1',m)
        row=json.loads(config.LOG_FILE.read_text().splitlines()[-1])
        self.assertEqual(row['correlationId'],m['correlationId'])
        self.assertNotIn('secret text',json.dumps(row))

class ProcessTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir=ROOT); self.dir=Path(self.tmp.name); self.processes=[]
        with socket.socket() as sock: sock.bind(('127.0.0.1',0)); self.port=sock.getsockname()[1]
        self.env=dict(os.environ,BROKER_HOST='127.0.0.1',BROKER_STATE_FILE=str(self.dir/'state.json'),
                      CONSUMER_INBOX_FILE=str(self.dir/'inbox.db'),BROKER_LOG_FILE=str(self.dir/'log.jsonl'),
                      DELIVERY_DELAY='0.1',ACK_TIMEOUT='0.25',BACKOFF_BASE='0.05',BACKOFF_CAP='0.2',RETRY_JITTER='0.1')
        self.start_broker()
    def launch(self,script,*args,env=None):
        p=subprocess.Popen([sys.executable,str(ROOT/script),*args],cwd=ROOT,env=env or self.env,
                           stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,
                           creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        self.processes.append(p); return p
    def wait_until(self,predicate,timeout=8):
        end=time.monotonic()+timeout
        while time.monotonic()<end:
            try:
                if predicate(): return
            except (OSError,ValueError,KeyError,sqlite3.OperationalError): pass
            time.sleep(.05)
        self.fail('Condition not reached within timeout')
    def start_broker(self):
        self.broker=self.launch('broker.py','--port',str(self.port))
        self.wait_until(lambda:check('127.0.0.1',self.port)['ready'])
    def data(self): return json.loads((self.dir/'state.json').read_text())
    def consumer(self,crash=False):
        env=dict(self.env,CONSUMER_CRASH_AFTER_COMMIT='1' if crash else '0')
        p=self.launch('subscriber.py','--id','sub1','--publisher','pub1','--host','127.0.0.1','--port',str(self.port),env=env)
        self.wait_until(lambda:self.data()['subscribers'].get('sub1',{}).get('publisher')=='pub1')
        return p
    def stop(self,p):
        if p.poll() is None: p.terminate()
        p.wait(timeout=4)
    def tearDown(self):
        for p in reversed(self.processes):
            self.stop(p)
            if p.stderr: p.stderr.close()
        self.tmp.cleanup()
    def test_real_crash_after_commit_before_ack_then_broker_restart(self):
        consumer=self.consumer(crash=True); m=create('persistent effect')
        publish_request(m,'pub1','127.0.0.1',self.port)
        consumer.wait(timeout=6); self.assertEqual(consumer.returncode,86)
        self.assertEqual(len(self.data()['subscribers']['sub1']['pending']),1)
        self.stop(self.broker); self.start_broker()
        self.consumer()
        self.wait_until(lambda:not self.data()['subscribers']['sub1']['pending'])
        with closing(sqlite3.connect(self.dir/'inbox.db')) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM inbox_v1').fetchone()[0],1)
    def test_offline_delivery_and_duplicate_publish(self):
        consumer=self.consumer(); self.stop(consumer)
        m=create('offline'); publish_request(m,'pub1','127.0.0.1',self.port)
        # Allow old publisher handler to finish before reconnecting with same ID.
        time.sleep(.1)
        publish_request(m,'pub1','127.0.0.1',self.port)
        self.assertEqual(len(self.data()['subscribers']['sub1']['pending']),1)
        self.consumer(); self.wait_until(lambda:not self.data()['subscribers']['sub1']['pending'])
    def test_live_health_does_not_change_state(self):
        before=(self.dir/'state.json').read_bytes()
        self.assertTrue(check('127.0.0.1',self.port)['ready'])
        self.assertEqual(before,(self.dir/'state.json').read_bytes())
    def test_no_ack_is_bounded_and_dlq_unblocks_next_message(self):
        with socket.create_connection(('127.0.0.1',self.port),timeout=5) as sock:
            with sock.makefile('r',encoding='utf-8') as reader:
                sock.sendall(b'SUBSCRIBER:sub1\n'); self.assertEqual(reader.readline().strip(),'OK:CONNECTED')
                sock.sendall(b'SUBSCRIBE:pub1\n'); reader.readline()
                first=create('first'); second=create('second')
                publish_request(first,'pub1','127.0.0.1',self.port); time.sleep(.1)
                publish_request(second,'pub1','127.0.0.1',self.port)
                received=[json.loads(reader.readline()[8:]) for _ in range(4)]
                self.assertEqual([m['messageId'] for m in received[:3]],[first['messageId']]*3)
                self.assertEqual(received[3]['messageId'],second['messageId'])
                self.assertEqual(self.data()['dlq'][0]['message']['attempts'],3)
    def test_gui_publisher_outbox_is_confirmed_and_durable(self):
        from publisher import Publisher
        with patch.object(client_core,'INBOX_FILE',self.dir/'gui.db'):
            pub=Publisher(SimpleNamespace(events=queue.Queue()),'guiPub','127.0.0.1',self.port)
            try:
                pub.publish('from GUI')
                def accepted():
                    with closing(sqlite3.connect(self.dir/'gui.db')) as db:
                        return db.execute("SELECT COUNT(*) FROM outbox_v1 WHERE status='accepted'").fetchone()[0]==1
                self.wait_until(accepted)
            finally:
                pub.close(); pub.reader_thread.join(2); pub.publisher_thread.join(2)

    def test_subscribers_have_separate_explicit_local_ports(self):
        from subscriber import Subscriber
        with patch.object(client_core,'INBOX_FILE',self.dir/'ports.db'), patch.object(config,'LOG_FILE',self.dir/'ports.log.jsonl'):
            clients=[]
            try:
                for index in range(2):
                    with socket.socket() as candidate:
                        candidate.bind(('127.0.0.1',0)); local_port=candidate.getsockname()[1]
                    client=Subscriber(SimpleNamespace(events=queue.Queue()),f'ports{index}','127.0.0.1',self.port,'pub1',local_port=local_port)
                    clients.append(client)
                    self.assertEqual(client.local_address[1],local_port)
                    self.assertEqual(client.broker_address[1],self.port)
                self.assertNotEqual(clients[0].local_address[1],clients[1].local_address[1])
                self.wait_until(lambda:all(self.data()['subscribers'].get(f'ports{i}',{}).get('publisher')=='pub1' for i in range(2)))
                message=create('both subscribers')
                publish_request(message,'pub1','127.0.0.1',self.port)
                self.wait_until(lambda:all(not self.data()['subscribers'][f'ports{i}']['pending'] for i in range(2)))
                with closing(sqlite3.connect(self.dir/'ports.db')) as db:
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM inbox_v1').fetchone()[0],2)
                for invalid in (-1,65536):
                    with self.assertRaises(ValueError):
                        Subscriber(SimpleNamespace(events=queue.Queue()),'bad','127.0.0.1',self.port,local_port=invalid)
                with self.assertRaises(OSError):
                    Subscriber(SimpleNamespace(events=queue.Queue()),'occupied','127.0.0.1',self.port,local_port=self.port)
            finally:
                for client in clients: client.close()
                for client in clients: client.reader_thread.join(2)

    def test_publishers_have_separate_explicit_local_ports(self):
        from publisher import Publisher
        with patch.object(client_core,'INBOX_FILE',self.dir/'publishers.db'), patch.object(config,'LOG_FILE',self.dir/'publishers.log.jsonl'):
            clients=[]
            try:
                for index in range(2):
                    with socket.socket() as candidate:
                        candidate.bind(('127.0.0.1',0)); local_port=candidate.getsockname()[1]
                    client=Publisher(SimpleNamespace(events=queue.Queue()),f'localpub{index}','127.0.0.1',self.port,local_port=local_port)
                    clients.append(client)
                    self.assertEqual(client.local_address[1],local_port)
                    self.assertEqual(client.broker_address[1],self.port)
                    client.publish(f'message from localpub{index}')
                self.assertNotEqual(clients[0].local_address[1],clients[1].local_address[1])
                def accepted():
                    with closing(sqlite3.connect(self.dir/'publishers.db')) as db:
                        return db.execute("SELECT COUNT(*) FROM outbox_v1 WHERE status='accepted'").fetchone()[0]==2
                self.wait_until(accepted)
                self.assertEqual({v['publisher'] for v in self.data()['accepted'].values()},{'localpub0','localpub1'})
                for invalid in (-1,65536):
                    with self.assertRaises(ValueError):
                        Publisher(SimpleNamespace(events=queue.Queue()),'invalid','127.0.0.1',self.port,local_port=invalid)
                with self.assertRaises(OSError):
                    Publisher(SimpleNamespace(events=queue.Queue()),'occupied','127.0.0.1',self.port,local_port=self.port)
            finally:
                for client in clients: client.close()
                for client in clients:
                    client.reader_thread.join(2); client.publisher_thread.join(2)

if __name__=='__main__': unittest.main()
