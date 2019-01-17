"""Hysteresis queue for use with Twisted or asyncio"""

import queue
try:
    import asyncio
    #If we run under Python 3.x (x>4), define an asyncio hysteresis queue class
    class _AioSoon(object):
        """Helper class for making core hysteresis queue event framework agnostic"""
        # pylint: disable=too-few-public-methods
        def __call__(self, callback, argument):
            print("DEBUG soon:", type(callback), argument, callback)
            asyncio.get_event_loop().call_later(0.0, callback, argument)
    class AioHysteresisQueue(object):
        """Asyncio based hysteresis queue wrapper"""
        def __init__(self, low=8000, high=10000, event_handler=None):
            self.core = _CoreHysteresisQueue(_AioSoon(), low, high, event_handler)
        def put(self, entry):
            """Add entry to the queue, returns boolean indicating success
                will invoke loop.call_later if there is a callback pending for
                the consumer handler."""
            return self.core.put(entry)
        def get(self, callback):
            """Fetch an entry from the queue, imediately if possible, or remember
               callback for when an entry becomes available."""
            self.core.get(callback)

    try:
        #Optionally also define a Twisted hysteresis queue class
        from twisted.internet import task
        from twisted.internet import reactor
        class _TxSoon(object):
            """Helper class for making core hysteresis queue event framework agnostic"""
            # pylint: disable=too-few-public-methods
            def __call__(self, callback, argument):
                task.deferLater(reactor, 0.0, callback, argument)
        class TxHysteresisQueue(object):
            """Twisted based hysteresis queue wrapper"""
            def __init__(self, low=8000, high=10000, event_handler=None):
                self.core = _CoreHysteresisQueue(_TxSoon(), low, high, event_handler)
            def put(self, entry):
                """Add entry to the queue, returns boolean indicating success
                    will invoke task.deferLater if there is a callback pending
                    for the consumer handler."""
                return self.core.put(entry)
            def get(self, callback):
                """Fetch an entry from the queue, imediately if possible,
                   or remember callback for when an entry becomes available."""
                self.core.get(callback)
    except ImportError:
        pass
except ImportError:
    #If we run on an older version of Python, the Twisted hysteresis queue
    # class is no longer optional
    from twisted.internet import task
    from twisted.internet import reactor
    class _TxSoon(object):
        """Helper class for making core hysteresis queue event framework agnostic"""
        # pylint: disable=too-few-public-methods
        def __call__(self, callback, argument):
            task.deferLater(reactor, 0.0, callback, argument)
    class TxHysteresisQueue(object):
        """Twisted based hysteresis queue wrapper"""
        def __init__(self, low=8000, high=10000, event_handler=None):
            self.core = _CoreHysteresisQueue(_TxSoon(), low, high, event_handler)
        def put(self, entry):
            """Add entry to the queue, returns boolean indicating success
                will invoke task.deferLater if there is a callback pending
                for the consumer handler."""
            return self.core.put(entry)
        def get(self, callback):
            """Fetch an entry from the queue, imediately if possible,
               or remember callback for when an entry becomes available."""
            self.core.get(callback)



class _CoreHysteresisQueue(object):
    """Simple Twisted based hysteresis queue"""
    def __init__(self, soon, low, high, event_handler):
        self.soon = soon
        self.low = low
        self.high = high
        self.active = True
        self.event_handler = event_handler
        self.msg_queue = queue.Queue()
        self.fetch_msg_queue = queue.Queue()
    def put(self, entry):
        """Add entry to the queue, returns boolean indicating success
        will invoke callLater if there is a callback pending for the consumer handler."""
        #Return false imediately if inactivated dueue to hysteresis setting.
        if self.active is False:
            return False
        try:
            #See if there is a callback waiting already
            callback = self.fetch_msg_queue.get(block=False)
        except queue.Empty:
            callback = None
        if callback:
            #If there is a callback waiting schedule for it to be called on
            # the earliest opportunity
            self.soon(callback, entry)
            return True
        else:
            #If no callback is waiting, add entry to the queue
            self.msg_queue.put(entry)
            if self.msg_queue.qsize() >= self.high:
                # Queue is full now (high watermark, disable adding untill empty.
                self.active = False
                #Call handler of high/low watermark events on earliest opportunity
                self.soon(self.event_handler, False)
            return True
    def  get(self, callback):
        """Fetch an entry from the queue, imediately if possible, or remember callback for when an
           entry becomes available."""
        try:
            #See if we can fetch a value from the queue right now.
            rval = self.msg_queue.get(block=False)
        except queue.Empty:
            rval = None
        if rval:
            #If we can, call callback at earliest opportunity
            self.soon(callback, rval)
            if self.active is False and self.msg_queue.qsize() <= self.low:
                #If adding to the queue was disabled and we just dropped below the low water mark,
                # re-enable the queue now.
                self.active = True
                #Call handler of high/low watermark events on earliest opportunity
                task.deferLater(reactor, 0.0, self.event_handler, True)
        else:
            # If the queue was empty, add our callback to the callback queue
            self.fetch_msg_queue.put(callback)
