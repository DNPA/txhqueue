#!/usr/bin/env python3
"""Basic testrun for AioHysteresisQueue with a producer that is slightly faster than the consumer"""
import asyncio
import datetime
from txhqueue import AioHysteresisQueue as HysteresisQueue

#pylint: disable=global-statement
#pylint: disable=invalid-name
consume_count = 0
low_water_count = 0
high_water_count = 0


def lowwatermark(count):
    """Low watermark callback"""
    global low_water_count
    low_water_count += 1
    now = datetime.datetime.now().isoformat()
    print(now, "Low water mark reached, re-activating HysteresisQueue. Dropcount = ", count)

def highwatermark(count):
    """High watermark callback"""
    global high_water_count
    high_water_count += 1
    now = datetime.datetime.now().isoformat()
    print(now, "High water mark reached, de-activating HysteresisQueue, OKcount = ", count)

def produce(hysteresis_queue, event_loop):
    """Fast produce function"""
    hysteresis_queue.put("har")
    event_loop.call_later(0.044, produce, hysteresis_queue, event_loop)


def consume(hysteresis_queue, event_loop):
    """Slow consume function"""
    def cb1(msg):
        #pylint: disable=unused-argument
        """Data callback of the consumer"""
        global consume_count
        global low_water_count
        global high_water_count
        consume_count += 1
        if consume_count == 1000:
            print("Done consuming 1000 entries from queue")
            if high_water_count != 3:
                print("WARNING: expected 3 high water events, received", high_water_count)
            if low_water_count != 3:
                print("WARNING: expected 3 low water events, received", low_water_count)
            event_loop.stop()
        event_loop.call_later(0.047, consume, hysteresis_queue, event_loop)
    hysteresis_queue.get(cb1)


#Instantiate the queue
HQUEUE = HysteresisQueue(low=5, high=25, highwater=highwatermark, lowwater=lowwatermark)
LOOP = asyncio.get_event_loop()
#Start the producer
produce(HQUEUE, LOOP)
#Start the consumer
consume(HQUEUE, LOOP)
print("Starting base test for AioHysteresisQueue, this should take about a minute.")
LOOP.run_forever()
LOOP.close()
