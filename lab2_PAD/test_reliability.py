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
import threading
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch
from contextlib import closing
import broker
import grpc
import broker_pb2 as pb
import broker_pb2_grpc as rpc
from common import to_proto, from_proto, connect
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
        with patch.object(config,'DELIVERY_DELAY',2),patch('broker.time.time',return_value=100):
            message=self.message()
        broker.online_subs['sub1']=broker.Sink()
        with patch.object(broker.Sink,'deliver') as send:
            with patch('broker.time.time',return_value=101.99): broker.flush('sub1')
            send.assert_not_called()
            broker.load()
            pending=broker.state['subscribers']['sub1']['pending'][0]
            self.assertEqual(pending['attempts'],0)
            self.assertEqual(pending['nextAttemptAt'],102)
            with patch('broker.time.time',return_value=102): broker.flush('sub1')
            self.assertEqual(send.call_count,1)
            self.assertEqual(message['messageId'],send.call_args.args[0]['messageId'])

    def test_unsubscribe_during_delay_moves_message_to_dlq(self):
        with patch.object(config,'DELIVERY_DELAY',2): message=self.message()
        broker.online_subs['sub1']=broker.Sink()
        with patch.object(broker.Sink,'deliver') as send:
            broker.flush('sub1')
            broker.handle('SUBSCRIBER','sub1','UNSUBSCRIBE')
            send.assert_not_called()
        broker.load()
        self.assertEqual(broker.state['subscribers']['sub1']['pending'],[])
        entry=broker.state['dlq'][0]
        self.assertEqual(entry['message']['messageId'],message['messageId'])
        self.assertEqual(entry['reason'],'UNSUBSCRIBED')
        self.assertEqual(entry['message']['attempts'],0)

    def test_protobuf_roundtrip_preserves_contract_and_extensions(self):
        message=create('Salut gRPC'); message['optional']={'flag':True,'number':3}
        message['payload']['extra']=['a','b']
        encoded=to_proto(message).SerializeToString()
        self.assertEqual(from_proto(pb.Message.FromString(encoded)),message)
        self.assertEqual(pb.DESCRIPTOR.services_by_name['MessageBroker'].methods_by_name['Watch'].server_streaming,True)

    def test_lost_reply_after_commit_is_safe_to_retry(self):
        from concurrent import futures
        committed=threading.Event(); release=threading.Event()
        class LostReply(rpc.MessageBrokerServicer):
            def PublishMessage(service,request,context):
                with broker.lock: broker.publish('pub1',from_proto(request.message))
                committed.set(); release.wait(2)
                return pb.PublishReply(message_id=request.message.message_id,recipients=1)
        server=grpc.server(futures.ThreadPoolExecutor(max_workers=2)); rpc.add_MessageBrokerServicer_to_server(LostReply(),server)
        port=server.add_insecure_port('127.0.0.1:0'); server.start()
        message=create('commit, lost reply',subject='pub1')
        try:
            with connect('127.0.0.1',port) as channel:
                stub=rpc.MessageBrokerStub(channel)
                with self.assertRaises(grpc.RpcError) as error:
                    stub.PublishMessage(pb.PublishRequest(message=to_proto(message)),timeout=.3)
                self.assertEqual(error.exception.code(),grpc.StatusCode.DEADLINE_EXCEEDED)
                self.assertTrue(committed.is_set()); release.set()
                response=stub.PublishMessage(pb.PublishRequest(message=to_proto(message)),timeout=2)
                self.assertEqual(response.message_id,message['messageId'])
                self.assertEqual(len(broker.state['subscribers']['sub1']['pending']),1)
        finally: release.set(); server.stop(0).wait()

    def test_unavailable_service_fallback_keeps_failed_request_durable(self):
        from concurrent import futures
        from publisher import Publisher
        class Unavailable(rpc.MessageBrokerServicer):
            def Register(service,request,context): return pb.RegisterReply(session_id='test-session')
            def Watch(service,request,context):
                yield pb.Delivery(heartbeat=True)
                while context.is_active(): time.sleep(.05)
            def PublishMessage(service,request,context): context.abort(grpc.StatusCode.UNAVAILABLE,'simulated')
            def Disconnect(service,request,context): return pb.Reply(detail='closed')
        server=grpc.server(futures.ThreadPoolExecutor(max_workers=3)); rpc.add_MessageBrokerServicer_to_server(Unavailable(),server)
        port=server.add_insecure_port('127.0.0.1:0'); server.start(); client=None
        with patch.object(config,'INBOX_FILE',Path(self.tmp.name)/'outbox.db'),patch.object(config,'ACK_TIMEOUT',.05),patch.object(config,'BACKOFF_BASE',0),patch.object(config,'BACKOFF_CAP',0):
            try:
                client=Publisher(SimpleNamespace(events=queue.Queue()),'fallback','127.0.0.1',port,subject='A')
                client.publish('keep this request')
                end=time.monotonic()+4; row=None
                while time.monotonic()<end:
                    with closing(config.outbox_db()) as db:
                        row=db.execute('SELECT status,attempts,payload FROM outbox_v1').fetchone()
                    if row and row[0]=='failed': break
                    time.sleep(.05)
                self.assertEqual(row[0],'failed'); self.assertEqual(row[1],config.MAX_ATTEMPTS)
                self.assertEqual(json.loads(row[2])['payload']['text'],'keep this request')
            finally:
                if client:
                    client.close(); client.reader_thread.join(3); client.publisher_thread.join(3)
                server.stop(0).wait()

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
        broker.online_subs['sub1']=broker.Sink()
        with patch.object(broker.Sink,'deliver',lambda c,message:sent.append(copy.deepcopy(message))):
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
        self.tmp=tempfile.TemporaryDirectory(dir=ROOT); self.dir=Path(self.tmp.name); self.processes=[]; self.clients=[]
        self.data_patches=[patch.object(config,'INBOX_FILE',self.dir/'inbox.db'),patch.object(config,'LOG_FILE',self.dir/'client.log.jsonl')]
        for item in self.data_patches: item.start()
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
        for client in self.clients: client.close()
        for client in self.clients:
            client.reader_thread.join(4)
            if hasattr(client,'publisher_thread'): client.publisher_thread.join(4)
        for p in reversed(self.processes):
            self.stop(p)
            if p.stderr: p.stderr.close()
        for item in reversed(self.data_patches): item.stop()
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
        channel=connect('127.0.0.1',self.port); stub=rpc.MessageBrokerStub(channel)
        token=stub.Register(pb.RegisterRequest(role=pb.SUBSCRIBER,name='sub1'),timeout=3).session_id
        stream=stub.Watch(pb.Session(session_id=token),timeout=6)
        try:
            self.assertTrue(next(stream).heartbeat)
            stub.Follow(pb.FollowRequest(session_id=token,publisher='pub1'),timeout=3)
            first=create('first'); second=create('second')
            publish_request(first,'pub1','127.0.0.1',self.port)
            publish_request(second,'pub1','127.0.0.1',self.port)
            received=[]
            while len(received)<4:
                event=next(stream)
                if not event.heartbeat: received.append(event)
            self.assertEqual([m.message.message_id for m in received[:3]],[first['messageId']]*3)
            self.assertEqual(received[3].message.message_id,second['messageId'])
            self.assertEqual(self.data()['dlq'][0]['message']['attempts'],3)
        finally:
            stub.Disconnect(pb.Session(session_id=token),timeout=3); stream.cancel(); channel.close()

    def test_gui_publisher_outbox_is_confirmed_and_durable(self):
        from publisher import Publisher
        with patch.object(client_core,'INBOX_FILE',self.dir/'gui.db'), patch.object(config,'LOG_FILE',self.dir/'gui.log.jsonl'):
            pub=Publisher(SimpleNamespace(events=queue.Queue()),'guiPub','127.0.0.1',self.port)
            try:
                pub.publish('from GUI')
                def accepted():
                    with closing(sqlite3.connect(self.dir/'gui.db')) as db:
                        return db.execute("SELECT COUNT(*) FROM outbox_v1 WHERE status='accepted'").fetchone()[0]==1
                self.wait_until(accepted)
            finally:
                pub.close(); pub.reader_thread.join(2); pub.publisher_thread.join(2)

    def test_rpc_validation_roles_conflicts_and_duplicate_registration(self):
        with connect('127.0.0.1',self.port) as channel:
            stub=rpc.MessageBrokerStub(channel)
            token=stub.Register(pb.RegisterRequest(role=pb.PUBLISHER,name='rawPub'),timeout=3).session_id
            try:
                with self.assertRaises(grpc.RpcError) as error:
                    stub.Publish(pb.PublishRequest(session_id=token,message=pb.Message()),timeout=3)
                self.assertEqual(error.exception.code(),grpc.StatusCode.INVALID_ARGUMENT)
                message=create('valid')
                stub.Publish(pb.PublishRequest(session_id=token,message=to_proto(message)),timeout=3)
                message['payload']['text']='conflict'
                with self.assertRaises(grpc.RpcError) as error:
                    stub.Publish(pb.PublishRequest(session_id=token,message=to_proto(message)),timeout=3)
                self.assertEqual(error.exception.code(),grpc.StatusCode.ALREADY_EXISTS)
                with self.assertRaises(grpc.RpcError) as error:
                    stub.Register(pb.RegisterRequest(role=pb.PUBLISHER,name='rawPub'),timeout=3)
                self.assertEqual(error.exception.code(),grpc.StatusCode.ALREADY_EXISTS)
                with self.assertRaises(grpc.RpcError) as error:
                    stub.Follow(pb.FollowRequest(session_id=token,publisher='pub1'),timeout=3)
                self.assertEqual(error.exception.code(),grpc.StatusCode.PERMISSION_DENIED)
                self.assertTrue(stub.Health(pb.Empty(),timeout=3).ready)
            finally: stub.Disconnect(pb.Session(session_id=token),timeout=3)

    def test_rpc_unsubscribe_before_delivery_and_stale_session(self):
        with connect('127.0.0.1',self.port) as channel:
            stub=rpc.MessageBrokerStub(channel)
            token=stub.Register(pb.RegisterRequest(role=pb.SUBSCRIBER,name='pending'),timeout=3).session_id
            stub.Follow(pb.FollowRequest(session_id=token,publisher='pub1'),timeout=3)
            # Without Watch this subscriber is offline, so publication cannot be ACKed.
            publish_request(create('to DLQ'),'pub1','127.0.0.1',self.port)
            stub.Unsubscribe(pb.Session(session_id=token),timeout=3)
            self.assertEqual(self.data()['dlq'][0]['reason'],'UNSUBSCRIBED')
            stub.Disconnect(pb.Session(session_id=token),timeout=3)
            with self.assertRaises(grpc.RpcError) as error:
                stub.Status(pb.Session(session_id=token),timeout=3)
            self.assertEqual(error.exception.code(),grpc.StatusCode.UNAUTHENTICATED)

    def test_rpc_nack_and_subscriber_status(self):
        with connect('127.0.0.1',self.port) as channel:
            stub=rpc.MessageBrokerStub(channel)
            token=stub.Register(pb.RegisterRequest(role=pb.SUBSCRIBER,name='rawSub'),timeout=3).session_id
            stream=stub.Watch(pb.Session(session_id=token),timeout=6)
            try:
                next(stream); stub.Follow(pb.FollowRequest(session_id=token,publisher='pub1'),timeout=3)
                message=create('bad effect'); publish_request(message,'pub1','127.0.0.1',self.port)
                event=next(stream)
                while event.heartbeat: event=next(stream)
                stub.Nack(pb.Confirmation(session_id=token,message_id=message['messageId'],transient=False),timeout=3)
                self.assertEqual(stub.Status(pb.Session(session_id=token),timeout=3).pending,0)
                self.assertEqual(self.data()['dlq'][0]['reason'],'PROCESSING_FAILED')
            finally:
                stub.Disconnect(pb.Session(session_id=token),timeout=3); stream.cancel()

    def test_health_available_when_all_stream_slots_used(self):
        channel=connect('127.0.0.1',self.port); stub=rpc.MessageBrokerStub(channel); active=[]
        try:
            for i in range(broker.MAX_SESSIONS):
                token=stub.Register(pb.RegisterRequest(role=pb.SUBSCRIBER,name=f'load{i}'),timeout=3).session_id
                stream=stub.Watch(pb.Session(session_id=token),timeout=15); next(stream)
                active.append((token,stream))
            self.assertTrue(stub.Health(pb.Empty(),timeout=3).ready)
            with self.assertRaises(grpc.RpcError) as error:
                stub.Register(pb.RegisterRequest(role=pb.PUBLISHER,name='overflow'),timeout=3)
            self.assertEqual(error.exception.code(),grpc.StatusCode.RESOURCE_EXHAUSTED)
        finally:
            for token,stream in active:
                stub.Disconnect(pb.Session(session_id=token),timeout=3); stream.cancel()
            channel.close()

    def node(self,role,name,subject,history=False):
        from publisher import Publisher
        from subscriber import Subscriber
        app=SimpleNamespace(events=queue.Queue())
        client=Publisher(app,name,'127.0.0.1',self.port,subject=subject) if role=='publisher' else Subscriber(app,name,'127.0.0.1',self.port,subject=subject,load_history=history)
        self.clients.append(client); return client

    def inbox_count(self,name):
        if not (self.dir/'inbox.db').exists(): return 0
        with closing(sqlite3.connect(self.dir/'inbox.db')) as db:
            try: return db.execute('SELECT COUNT(*) FROM inbox_v1 WHERE subscriber=?',(name,)).fetchone()[0]
            except sqlite3.OperationalError: return 0

    def test_topic_routes_multiple_publishers_to_only_matching_subscribers(self):
        first=self.node('publisher','first','A'); second=self.node('publisher','second','A')
        self.node('subscriber','a1','A'); self.node('subscriber','a2','A'); self.node('subscriber','b','B')
        first.publish('from first'); second.publish('from second')
        self.wait_until(lambda:self.inbox_count('a1')==2 and self.inbox_count('a2')==2)
        self.assertEqual(self.inbox_count('b'),0)
        with connect('127.0.0.1',self.port) as channel:
            topics=rpc.MessageBrokerStub(channel).ListTopics(pb.Empty(),timeout=2).subjects
            self.assertIn('A',topics); self.assertIn('B',topics)

    def test_optional_history_replay_does_not_repeat_local_effect(self):
        for content in ('old 1','old 2'):
            publish_request(create(content,subject='History'),'pub1','127.0.0.1',self.port)
        no_history=self.node('subscriber','no-history','History')
        historical=self.node('subscriber','history','History',history=True)
        self.wait_until(lambda:self.inbox_count('history')==2)
        self.assertEqual(self.inbox_count('no-history'),0)
        self.wait_until(lambda:not self.data()['subscribers']['history']['pending'])
        historical.close(); historical.reader_thread.join(3)
        replay=self.node('subscriber','history','History',history=True)
        self.wait_until(lambda:not self.data()['subscribers']['history']['pending'])
        self.assertEqual(self.inbox_count('history'),2)
        events=[]
        while not replay.app.events.empty(): events.append(replay.app.events.get_nowait())
        self.assertEqual(sum('ISTORIC' in e[3] for e in events if e[0]=='log'),2)
        publish_request(create('new',subject='History'),'pub1','127.0.0.1',self.port)
        self.wait_until(lambda:self.inbox_count('history')==3 and self.inbox_count('no-history')==1)

    def test_cancellation_closes_stream_but_keeps_subscription(self):
        client=self.node('subscriber','cancelled','Cancel'); token=client.session
        client.close(); client.reader_thread.join(3)
        with connect('127.0.0.1',self.port) as channel:
            with self.assertRaises(grpc.RpcError) as error:
                rpc.MessageBrokerStub(channel).Status(pb.Session(session_id=token),timeout=2)
            self.assertEqual(error.exception.code(),grpc.StatusCode.UNAUTHENTICATED)
        publish_request(create('offline after cancel',subject='Cancel'),'pub1','127.0.0.1',self.port)
        self.assertEqual(self.data()['subscribers']['cancelled']['pending'][0]['attempts'],0)
        self.assertEqual(self.inbox_count('cancelled'),0)
        self.node('subscriber','cancelled','Cancel')
        self.wait_until(lambda:self.inbox_count('cancelled')==1)

    def test_subject_version_and_unsubscribe_before_change(self):
        client=self.node('subscriber','switch','A')
        with self.assertRaises(OSError): client.send('SUBSCRIBE:B')
        client.send('UNSUBSCRIBE'); client.send('SUBSCRIBE:B')
        self.assertEqual(client.stub.Status(pb.Session(session_id=client.session),timeout=2).subject,'B')
        with connect('127.0.0.1',self.port) as channel:
            token=rpc.MessageBrokerStub(channel).Register(pb.RegisterRequest(role=pb.PUBLISHER,name='version'),timeout=2).session_id
            stub=rpc.MessageBrokerStub(channel)
            try:
                message=to_proto(create('bad version',subject='B')); message.schema_version=2
                with self.assertRaises(grpc.RpcError) as error:
                    stub.PublishMessage(pb.PublishRequest(session_id=token,message=message),timeout=2)
                self.assertEqual(error.exception.code(),grpc.StatusCode.INVALID_ARGUMENT)
            finally: stub.Disconnect(pb.Session(session_id=token),timeout=2)

if __name__=='__main__': unittest.main()
