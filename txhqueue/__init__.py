"""Hysteresis queue for use with Twisted or asyncio"""

try:
    from twisted.internet import task
    from twisted.internet import reactor
    from twisted.internet import defer
    from twisted.internet.task import LoopingCall
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
    # Pylint Rational:
    #    Simple wrapper to make future act like a deferred from code code,
    #    one method is all it takes.
    #pylint: disable=too-few-public-methods
    """Simple wrapper for giving Future a Deferred compatible callback"""
    def __init__(self, future):
        self.future = future
    def callback(self, value):
        """Call set_result on future instead of callback on a Deferred"""
        self.future.set_result(value)

class _AioSoon(object):
    """Helper class for making core hysteresis queue event framework agnostic"""
    # Pylint Rational:
    #    Simple function object like class for wrapping event framework timers.
    # pylint: disable=too-few-public-methods
    def __call__(self, callback, argument):
        asyncio.get_event_loop().call_later(0.0, callback, argument)
    def repeat(self, intervall, command):
        """Unimplemented"""
        pass #Currently not implemented for asyncio

class _TxSoon(object):
    """Helper class for making core hysteresis queue event framework agnostic"""
    # Pylint Rational:
    #    Simple function object like class for wrapping event framework timers.
    # pylint: disable=too-few-public-methods,no-self-use
    def __call__(self, callback, argument):
        task.deferLater(reactor, 0.0, callback, argument)
    def repeat(self, interval, command):
        """Call command every interval seconds"""
        LoopingCall(command).start(interval)

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
        future = asyncio.Future()
        if callback:
            future.add_done_callback(callback)
        self.core.get(_AioFutureWrapper(future))
        return future

class TxHysteresisQueue(object):
    """Twisted based hysteresis queue wrapper"""
    def __init__(self,
                 low=8000, high=10000,
                 highwater=None, lowwater=None,
                 flowstat_cb=None, flowstat_interval=60):
        # Pylint Rational:
        #    Constructor used to configure HysteresisQueue, we might have gone for a config dict,
        #    and maybe we should have, but with sane defaults, current implementation seems
        #    acceptable.
        #pylint: disable=too-many-arguments
        if not HAS_TWISTED:
            raise RuntimeError("Can not instantiate TxHysteresisQueue without twisted")
        self.core = _CoreHysteresisQueue(_TxSoon(),
                                         low, high,
                                         highwater, lowwater,
                                         flowstat_cb, flowstat_interval)
    def put(self, entry):
        """Add entry to the queue, returns boolean indicating success
            will invoke task.deferLater if there is a callback pending
            for the consumer handler."""
        return self.core.put(entry)
    def get(self, callback=None):
        """Fetch an entry from the queue, imediately if possible,
           or remember callback for when an entry becomes available."""
        deferred = defer.Deferred()
        if callback:
            deferred.addCallback(callback)
        self.core.get(deferred)
        return deferred



class _CoreHysteresisQueue(object):
    #We should fix this with closures later:
    # Pylint Rational:
        #    Constructor used to configure HysteresisQueue, we might have gone for a config dict,
        #    and maybe we should have, but with sane defaults, current implementation seems
        #    acceptable.
    #pylint: disable=too-many-instance-attributes
    """Simple Twisted based hysteresis queue"""
    def __init__(self, soon, low, high, highwater, lowwater, flowstat_cb=None,
                 flowstat_interval=60):
        # Pylint Rational:
        #    Constructor used to configure HysteresisQueue, we might have gone for a config dict,
        #    and maybe we should have, but with sane defaults, current implementation seems
        #    acceptable.
        #pylint: disable=too-many-arguments
        self.soon = soon
        self.low = low
        self.high = high
        self.active = True
        self.highwater = highwater
        self.lowwater = lowwater
        self.flowstat_callback = flowstat_cb
        self.msg_queue = list()
        self.fetch_msg_queue = list()
        self.dropcount = 0
        self.okcount = 0
        self.flowstat = dict()
        self.flowstat["produced"] = 0
        self.flowstat["consumed"] = 0
        self.flowstat["dropped"] = 0
        if flowstat_cb:
            soon.repeat(flowstat_interval, self.flow_stat_tick)
    def flow_stat_tick(self):
        """This method is called periodically if constructor specifies flowstat_cb.
        This method will call the specified callback with inflow, outflow and drop stats."""
        curstat = self.flowstat
        self.flowstat_callback(curstat)
    def put(self, entry):
        """Add entry to the queue, returns boolean indicating success
        will invoke callLater if there is a callback pending for the consumer handler."""
        #Return false imediately if inactivated dueue to hysteresis setting.
        if self.active is False:
            self.dropcount += 1
            self.flowstat["dropped"] += 1
            return False
        self.flowstat["produced"] += 1
        self.okcount += 1
        try:
            #See if there is a callback waiting already
            deferred = self.fetch_msg_queue.pop(0)
        except IndexError:
            deferred = None
        if deferred:
            #If there is a callback waiting scheduled for it to be called on
            # the earliest opportunity
            self.soon(deferred.callback, entry)
            self.flowstat["consumed"] += 1
            return True
        else:
            #If no callback is waiting, add entry to the queue
            self.msg_queue.append(entry)
            if len(self.msg_queue) >= self.high:
                # Queue is full now (high watermark, disable adding untill empty.
                self.active = False
                #Call handler of high/low watermark events on earliest opportunity
                self.soon(self.highwater, self.okcount)
                self.okcount = 0
            return True
    def  get(self, deferred):
        """Fetch an entry from the queue, imediately if possible, or remember callback for when an
           entry becomes available."""
        try:
            #See if we can fetch a value from the queue right now.
            rval = self.msg_queue.pop(0)
        except IndexError:
            rval = None
        if rval:
            #If we can, call callback at earliest opportunity
            self.soon(deferred.callback, rval)
            self.flowstat["consumed"] += 1
            if self.active is False and len(self.msg_queue) <= self.low:
                #If adding to the queue was disabled and we just dropped below the low water mark,
                # re-enable the queue now.
                self.active = True
                #Call handler of high/low watermark events on earliest opportunity
                self.soon(self.lowwater, self.dropcount)
                self.dropcount = 0
        else:
            # If the queue was empty, add our callback to the callback queue
            self.fetch_msg_queue.append(deferred)
