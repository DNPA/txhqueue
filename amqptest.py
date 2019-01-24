#!/usr/bin/env python3
"""Some test code for the TxAmqpForwarder  consumer.

This isn't a stand alone test, please check:

  http://localhost:15672/#/exchanges/%2F/foo


"""
import time
import datetime
from twisted.internet import reactor
from twisted.internet.task import LoopingCall, deferLater
from txhqueue import TxHysteresisQueue as HysteresisQueue
from txhqueue.consumer import TxAmqpForwarder


def lowwatermark(dropcount):
    """Low watermark callback"""
    now = datetime.datetime.now().isoformat()
    print(now, "Low water mark reached, re-activating HysteresisQueue. Drop count =", dropcount)

def highwatermark(okcount):
    """High watermark callback"""
    now = datetime.datetime.now().isoformat()
    print(now, "High water mark reached, de-activating HysteresisQueue. OK count =", okcount)

def produce(hqueue):
    """Fast produce function"""
    hqueue.put('{"msg": "har"}')


def converter(msgin, callback):
    "Dummy converter"
    callback(msgin.upper())

class RateCorrectedProduce(object):
    #pylint: disable=too-few-public-methods
    """Helper class for repairing inaccuracy of twisted.internet.task.LoopingCall"""
    def __init__(self, hqueue, interval):
        self.hqueue = hqueue
        self.interval = interval
        self.starttime = time.time()
        self.startcount = 0
    def __call__(self):
        self.startcount += 1
        produce(self.hqueue)
        expectedcount = round((time.time() - self.starttime)/self.interval)
        while self.startcount < expectedcount:
            self.startcount += 1
            produce(self.hqueue)

HQUEUE = HysteresisQueue(low=16000, high=20000, highwater=highwatermark, lowwater=lowwatermark)
CONSUMER = TxAmqpForwarder(HQUEUE,)

INTERVAL = 0.0001
RCP = RateCorrectedProduce(HQUEUE, INTERVAL)
LC = LoopingCall(RCP)
deferLater(reactor, 0.2, LC.start, INTERVAL)
#pylint: disable=no-member
reactor.run()
