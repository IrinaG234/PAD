from common import GRPCClient

class Subscriber(GRPCClient):
    def __init__(self, app, name, host, port, publisher="", subject=None, load_history=False):
        super().__init__(app, "SUBSCRIBER", name, host, port, publisher, subject=subject, load_history=load_history)

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
    parser.add_argument('--port',type=int,default=config.PORT); parser.add_argument('--subject'); parser.add_argument('--history',action='store_true'); args=parser.parse_args()
    app=SimpleNamespace(events=queue.Queue())
    client=Subscriber(app,args.id,args.host,args.port,args.publisher,subject=args.subject,load_history=args.history)
    try:
        while client.active:
            try: print(app.events.get(timeout=0.2),flush=True)
            except queue.Empty: pass
    except KeyboardInterrupt: pass
    finally: client.close()
