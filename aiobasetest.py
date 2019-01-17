#!/usr/bin/env python3
import asyncio
from txhqueue import AioHysteresisQueue as HysteresisQueue

def watermark(low):
    if low:
        print("Low water mark reached, re-activating HysteresisQueue")
    else:
        print("High water mark reached, de-activating HysteresisQueue")

def produce(hq, lp):
    ok = hq.put("har")
    print("Produced:", ok)
    lp.call_later(0.5, produce, hq, lp)


def consume(hq, lp):
    def cb1(msg):
        print("Consumed:", msg)
        lp.call_later(1.3, consume, hq, lp)
    hq.get(cb1)



hqueue = HysteresisQueue(low=5, high=10, event_handler=watermark)

loop = asyncio.new_event_loop()
produce(hqueue, loop)
consume(hqueue, loop)
loop.run_forever()
loop.close()
