#!/usr/bin/env python3
from txhqueue import TxHysteresisQueue as HysteresisQueue
from twisted.internet import reactor
from twisted.internet.task import LoopingCall,deferLater

def watermark(low):
    if low:
        print("Low water mark reached, re-activating HysteresisQueue")
    else:
        print("High water mark reached, de-activating HysteresisQueue")

def produce(hqueue):
    ok = hqueue.put("har")
    print("Produced:", ok)

def consume(hqueue):
    def cb1(msg):
        print("Consumed:", msg)
        deferLater(reactor,1.4,consume,hqueue)
    hqueue.get(cb1)



hqueue = HysteresisQueue(low=5, high=10, event_handler=watermark)

lc = LoopingCall(produce, hqueue)
lc.start(0.5)
consume(hqueue)
reactor.run()
