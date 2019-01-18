#Python  Hysteresis Queue for Twisted (and asyncio)

This library contains an asynchronous hysteresis queue implementation that works
with both the Twisted and the asyncio asynchonous frameworks. 

The Hysteresis Queue is meant to exist between a producer and a consumer running
in the same event loop and is meant especialy for situations where temporaraly 
dropping content is considered to be preferable over having the producer block its
operations. 

Before using the Hysteresis queue, you need to pick your own dominant asynchonous 
framework you want it to work with. If you don't know what to choose, Twisted is a 
great place to start as it is a time tested batteries included framework that works
on the largest range of Python versions and has a fast body of included and/or third
party libraries available to work with. Alternatively, if you know your code will only
need to work on Python 3.5 and above, and already know you won't be needing any protocols
only available in or for Twisted, asyncio, that comes with Python >= 3.5 can be a good
option too.

If you want to use the hysteresis queue with Twisted, start your code with:

```python
from txhqueue import TxHysteresisQueue as HysteresisQueue
```

For asyncio, instead use:
```python
from txhqueue import AioHysteresisQueue as HysteresisQueue
```

Now before you can instantiate your Hysteresis Queue, define two callables. One for high water events and one for low water events. The queue will continue accepting entries from the producer untill a high water mark is reached. At what point it will drop new entries untill the consumer has consumed enough entries for the queue to reach the low water mark. The callables we need to define are handlkers for high and low water events.

```python
def lowwatermark(dropcount):
    now = datetime.datetime.now().isoformat()
    print(now,"Low water mark reached, re-activating HysteresisQueue. Drop count =", dropcount)

def highwatermark(okcount):
    now = datetime.datetime.now().isoformat()
    print(now,"High water mark reached, de-activating HysteresisQueue. OK count =", okcount)
```

In a real world application using a logger might be more opertune, but the above gives you an idea of how things ought to fit.

Step three is instantiating our Hysteresis Queue

```python
hqueue = HysteresisQueue(low=2000, high=5000, highwater=highwatermark, lowwater=lowwatermark)
```

On instantiation we set the high and low water mark for the hysteresis queue and bind the queue to our two water mark event handlers.

Now for usage by the producer and the consumer. Usage by the producer is basically black hole. Just call the put method with the entity you want processed. The put method does return a boolean indicating if the entity was placed on the queue, but note there are only limited usecases where this value is of any use. Remember the queue takes care of counting drops and successfull puts and will output these counts in high water and low water events.

```python
is_queued = hq.put(entity)
```

For the consumer things do get asynchonous. The get method can either be used in an event framework agnostic way, or in an event framework specific way:

```python
#Event framework agnostic way
hq.get(consume_one)

#Twisted
d = hq.get()
d.addCallback(consume_one)

#Asyncio
f = hq.get()
f.add_done_callback(consume_one)
```

