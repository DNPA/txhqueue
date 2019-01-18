#!/usr/bin/env python3
import datetime
from txhqueue import TxHysteresisQueue as HysteresisQueue
from twisted.internet import reactor
from twisted.internet.task import LoopingCall,deferLater

def lowwatermark(dropcount):
    now = datetime.datetime.now().isoformat()
    print(now,"Low water mark reached, re-activating HysteresisQueue. Drop count =", dropcount)

def highwatermark(okcount):
    now = datetime.datetime.now().isoformat()
    print(now,"High water mark reached, de-activating HysteresisQueue. OK count =", okcount)

def produce(hqueue):
    ok = hqueue.put("har")

def consume(hqueue):
    def cb1(msg):
        deferLater(reactor,0.047,consume,hqueue)
    hqueue.get(cb1)



hqueue = HysteresisQueue(low=5, high=25, highwater=highwatermark, lowwater=lowwatermark)

lc = LoopingCall(produce, hqueue)
lc.start(0.044)
consume(hqueue)
reactor.run()
