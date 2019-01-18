#!/usr/bin/env python3
import asyncio
import datetime
from txhqueue import AioHysteresisQueue as HysteresisQueue

def lowwatermark(count):
    now = datetime.datetime.now().isoformat()
    print(now,"Low water mark reached, re-activating HysteresisQueue. Dropcount = ", count)

def highwatermark(count):
    now = datetime.datetime.now().isoformat()
    print(now,"High water mark reached, de-activating HysteresisQueue, OKcount = ", count)

def produce(hq, lp):
    ok = hq.put("har")
    lp.call_later(0.044, produce, hq, lp)


def consume(hq, lp):
    def cb1(msg):
        lp.call_later(0.047, consume, hq, lp)
    hq.get(cb1)



hqueue = HysteresisQueue(low=5, high=25, highwater=highwatermark, lowwater=lowwatermark)
loop = asyncio.get_event_loop()
produce(hqueue, loop)
consume(hqueue, loop)
loop.run_forever()
loop.close()
