from common import TCPClient

class Subscriber(TCPClient):
    def __init__(self, app, name, host, port, publisher="", local_port=0):
        super().__init__(app, "SUBSCRIBER", name, host, port, publisher, local_port=local_port)

    def subscribe(self, publisher):
        self.send("SUBSCRIBE:" + publisher)

    def unsubscribe(self):
        self.send("UNSUBSCRIBE")


"""Headless consumer used to reproduce crash-after-commit-before-ACK."""
import argparse
import queue
from types import SimpleNamespace
import common as config

if __name__=='__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('--id',default='sub1')
    parser.add_argument('--publisher',default='pub1'); parser.add_argument('--host',default=config.HOST)
    parser.add_argument('--port',type=int,default=config.PORT); parser.add_argument('--local-port',type=int,default=0); args=parser.parse_args()
    app=SimpleNamespace(events=queue.Queue())
    client=Subscriber(app,args.id,args.host,args.port,args.publisher,local_port=args.local_port)
    print(f'Local: {client.local_address} -> Broker: {client.broker_address}',flush=True)
    try:
        while client.active:
            try: print(app.events.get(timeout=0.2),flush=True)
            except queue.Empty: pass
    except KeyboardInterrupt: pass
    finally: client.close()
