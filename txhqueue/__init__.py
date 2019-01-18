"""Hysteresis queue for use with Twisted or asyncio"""

import queue
try:
    from twisted.internet import task
    from twisted.internet import reactor
    from twisted.internet import defer
    HAS_TWISTED = True
    try:
        import asyncio
        HAS_ASYNCIO = True
    except ImportError:
        pass
except ImportError:
    try:
        import asyncio
        HAS_ASYNCIO = True
    except ImportError:
        raise ImportError("Missing event loop framework (either Twisted or asyncio will do")

class _AioFutureWrapper(object):
    """Simple wrapper for giving Future a Deferred compatible callback"""
    def __init__(self, future):
        self.future = future
    def callback(self,value):
        self.future.set_result(value)

class _AioSoon(object):
    """Helper class for making core hysteresis queue event framework agnostic"""
    # pylint: disable=too-few-public-methods
    def __call__(self, callback, argument):
        asyncio.get_event_loop().call_later(0.0, callback, argument)

class _TxSoon(object):
    """Helper class for making core hysteresis queue event framework agnostic"""
    # pylint: disable=too-few-public-methods
    def __call__(self, callback, argument):
        task.deferLater(reactor, 0.0, callback, argument)

class AioHysteresisQueue(object):
    """Asyncio based hysteresis queue wrapper"""
    def __init__(self, low=8000, high=10000, highwater=None, lowwater=None):
        if not HAS_ASYNCIO:
            raise RuntimeError("Can not instantiate AioHysteresisQueue without asyncio")
        self.core = _CoreHysteresisQueue(_AioSoon(), low, high, highwater, lowwater)
    def put(self, entry):
        """Add entry to the queue, returns boolean indicating success
            will invoke loop.call_later if there is a callback pending for
            the consumer handler."""
        return self.core.put(entry)
    def get(self, callback=None):
        """Fetch an entry from the queue, imediately if possible, or remember
           callback for when an entry becomes available."""
        f=asyncio.Future()
        if callback:
            f.add_done_callback(callback)
        self.core.get(_AioFutureWrapper(f))
        return f

class TxHysteresisQueue(object):
    """Twisted based hysteresis queue wrapper"""
    def __init__(self, low=8000, high=10000, highwater=None, lowwater=None):
        if not HAS_TWISTED:
            raise RuntimeError("Can not instantiate TxHysteresisQueue without twisted")
        self.core = _CoreHysteresisQueue(_TxSoon(), low, high, highwater, lowwater)
    def put(self, entry):
        """Add entry to the queue, returns boolean indicating success
            will invoke task.deferLater if there is a callback pending
            for the consumer handler."""
        return self.core.put(entry)
    def get(self, callback=None):
        """Fetch an entry from the queue, imediately if possible,
           or remember callback for when an entry becomes available."""
        d = defer.Deferred()
        if callback:
            d.addCallback(callback)
        self.core.get(d)
        return d



class _CoreHysteresisQueue(object):
    """Simple Twisted based hysteresis queue"""
    def __init__(self, soon, low, high, highwater, lowwater):
        self.soon = soon
        self.low = low
        self.high = high
        self.active = True
        self.highwater = highwater
        self.lowwater = lowwater
        self.msg_queue = queue.Queue()
        self.fetch_msg_queue = queue.Queue()
        self.dropcount = 0
        self.okcount = 0
    def put(self, entry):
        """Add entry to the queue, returns boolean indicating success
        will invoke callLater if there is a callback pending for the consumer handler."""
        #Return false imediately if inactivated dueue to hysteresis setting.
        if self.active is False:
            self.dropcount += 1
            return False
        self.okcount +=1
        try:
            #See if there is a callback waiting already
            d = self.fetch_msg_queue.get(block=False)
        except queue.Empty:
            d = None
        if d:
            #If there is a callback waiting schedule for it to be called on
            # the earliest opportunity
            self.soon(d.callback, entry)
            return True
        else:
            #If no callback is waiting, add entry to the queue
            self.msg_queue.put(entry)
            if self.msg_queue.qsize() >= self.high:
                # Queue is full now (high watermark, disable adding untill empty.
                self.active = False
                #Call handler of high/low watermark events on earliest opportunity
                self.soon(self.highwater, self.okcount)
                self.okcount = 0
            return True
    def  get(self, d):
        """Fetch an entry from the queue, imediately if possible, or remember callback for when an
           entry becomes available."""
        try:
            #See if we can fetch a value from the queue right now.
            rval = self.msg_queue.get(block=False)
        except queue.Empty:
            rval = None
        if rval:
            #If we can, call callback at earliest opportunity
            self.soon(d.callback, rval)
            if self.active is False and self.msg_queue.qsize() <= self.low:
                #If adding to the queue was disabled and we just dropped below the low water mark,
                # re-enable the queue now.
                self.active = True
                #Call handler of high/low watermark events on earliest opportunity
                self.soon(self.lowwater, self.dropcount)
                self.dropcount = 0
        else:
            # If the queue was empty, add our callback to the callback queue
            self.fetch_msg_queue.put(d.callback)
