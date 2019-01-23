#!/usr/bin/env python3
import datetime
from txhqueue import TxHysteresisQueue as HysteresisQueue
from txhqueue.consumer import TxAmqpForwarder
from twisted.internet import reactor
from twisted.internet.task import LoopingCall,deferLater


def lowwatermark(dropcount):
    now = datetime.datetime.now().isoformat()
    print(now,"Low water mark reached, re-activating HysteresisQueue. Drop count =", dropcount)

def highwatermark(okcount):
    now = datetime.datetime.now().isoformat()
    print(now,"High water mark reached, de-activating HysteresisQueue. OK count =", okcount)

def produce(hqueue):
    ok = hqueue.put('{"msg": "har"}')

def converter(msgin):
    return msgin.upper()



hqueue = HysteresisQueue(low=40000, high=50000, highwater=highwatermark, lowwater=lowwatermark)
consumer = TxAmqpForwarder(hqueue,)


lc = LoopingCall(produce, hqueue)
lc.start(0.0005)
reactor.run()
