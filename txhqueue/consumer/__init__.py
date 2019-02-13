"""Pika RabitMQ consumer for thhqueue"""

try:
    import logging
    from twisted.internet import protocol, reactor, task
    from twisted.python import log
    from pika.adapters.twisted_connection import TwistedProtocolConnection
    from pika import ConnectionParameters, PlainCredentials, BasicProperties
    HAS_TWISTED_PIKA = True
except ImportError:
    HAS_TWISTED_PIKA = False

try:
    import asyncio
    import aio_pika
    HAS_AIO_PIKA = True
except ImportError:
    HAS_AIO_PIKA = False

##################################################################################################
#                                Non-ready asyncio implementation                                #
##################################################################################################
#class AioAmqpForwarder(object):
#    #pylint: disable=too-few-public-methods
#    #pylint: disable=too-many-instance-attributes
#    """Asyncio based AmqpForwardConsumer using aio_pika library"""
#     #pylint: disable=too-many-arguments
#    def __init__(self, hq, converter=None, host='localhost', port=5672, username="guest",
#                 password="guest", exchange='foo', routing_key='quz', window=16):
#        def loopback(original, callback):
#            """Standard do-nothing converter"""
#            callback(original)
#        if not HAS_AIO_PIKA:
#            raise RuntimeError("Can not instantiate AioPikaConsumer because of failing imports.")
#        #Remember hysteresis queue we need to be consuming from
#        self.hysteresis_queue = hq
#        #Set the conversion callback.
#        if converter is None:
#            #If none is specified, set it to a do nothing loopback
#            self.converter = loopback
#        else:
#            #Otherwise set the converter as specified in the constructor
#            self.converter = converter
#        #Server and port
#        self.host = host
#        self.port = port
#        #User credentials
#        self.username = username
#        self.password = password
#        #AMQP settings
#        self.exchange = exchange
#        self.routing_key = routing_key
#        self.window = window
#        self.running = True
#        self._connect()
#    def _connect(self):
#        """Connect or re-connect to AMQP server"""
#        def on_connect(connection_future):
#            """Handler that gets called when the connection is made or has failed"""
#            def on_channel(channel_future):
#                """Handler that gets called when the channel is ready"""
#                def on_exchange(exchange_future):
#                    """Handler that gets called when the exchange is ready or declaration
#                       failed"""
#                    try:
#                        self.exchange = exchange_future.result()
#                    #pylint: disable=broad-except
#                    except Exception:
#                        self.exchange = None
#                        asyncio.get_event_loop().stop()
#                    if self.exchange:
#                        #pylint: disable=unused-variable
#                        for wnum in range(0, self.window):
#                            #pylint: disable=unused-variable
#                            self.hysteresis_queue.get(self._process_body)
#
#                try:
#                    channel = channel_future.result()
#                #pylint: disable=broad-except
#                except Exception:
#                    channel = None
#                    asyncio.get_event_loop().stop()
#                if channel:
#                    amqp_exchange_future = asyncio.ensure_future(
#                        channel.declare_exchange(
#                            name=self.exchange,
#                            durable=True))
#                            #type=aio_pika.ExchangeType.HEADERS))
#                    amqp_exchange_future.add_done_callback(on_exchange)
#            try:
#                connection = connection_future.result()
#            #pylint: disable=broad-except
#            except Exception:
#                connection = None
#                asyncio.get_event_loop().stop()
#            if connection:
#                amqp_channel_future = asyncio.ensure_future(
#                    connection.channel())
#                amqp_channel_future.add_done_callback(on_channel)
#        amqp_connection_future = asyncio.ensure_future(
#            aio_pika.connect_robust(
#                host=self.host,
#                port=self.port,
#                login=self.username,
#                password=self.password,
#                loop=asyncio.get_event_loop()))
#        amqp_connection_future.add_done_callback(on_connect)
#    def _process_body(self, body_future):
#        def process_converted(converted_body):
#            """Callback for body after convert"""
#            def on_published(done_future):
#                """Callback for publish ready or failed"""
#                try:
#                    done_future.result()
#                    self.hysteresis_queue.get(self._process_body)
#                #pylint: disable=broad-except
#                except Exception:
#                    self.running = False
#                    asyncio.get_event_loop().stop()
#            if isinstance(converted_body, (bytes, str)):
#                publish_done_future = self.exchange.publish(
#                    aio_pika.Message(converted_body),
#                    routing_key=self.routing_key)
#                publish_done_future.add_done_callback(on_published)
#            else:
#                raise TypeError("converter should produce string or bytes, not",
#                                type(converted_body))
#        body = body_future.result()
#        if self.running:
#            try:
#                self.converter(body, process_converted)
#            #pylint: disable=broad-except
#            except Exception:
#                self.running = False
#                asyncio.get_event_loop().stop()
##################################################################################################


class TxAmqpForwarder(object):
    """Twisted based AmqpForwardConsumer using pica TwistedProtocolConnection"""
    # Pylint Rational:
    #   Lazy programmer; we may want to look at this and use one or two config dicts.
    #pylint: disable=too-many-instance-attributes
    # Pylint Rational:
    #   The AmqpForwarder is a stand alone active object without any public methods.
    #   The constructor is the whole public interface.
    #pylint: disable=too-few-public-methods
    def __init__(self, hq, converter=None, host='localhost', port=5672, username="guest",
                 password="guest", exchange='foo', routing_key='quz', window=16):
        # Pylint Rational:
        #   Lazy programmer; we may want to look at this and use one or two config dicts.
        #pylint: disable=too-many-arguments
        def loopback(original, callback):
            """Standard do-nothing converter"""
            callback(original)
        if not HAS_TWISTED_PIKA:
            raise RuntimeError("Can not instantiate TxPikaConsumer because of failing imports.")
        #Remember hysteresis queue we need to be consuming from
        self.hysteresis_queue = hq
        #Set the conversion callback.
        if converter is None:
            #If none is specified, set it to a do nothing loopback
            self.converter = loopback
        else:
            #Otherwise set the converter as specified in the constructor
            self.converter = converter
        #Server and port
        self.host = host
        self.port = port
        #User credentials
        self.username = username
        self.password = password
        #AMQP settings
        self.exchange = exchange
        self.routing_key = routing_key
        self.window = window
        #Set members used later to None
        self.client = None
        self.connect_deferred = None
        self.publish_deferred = None
        self.channel = None
        self.running = True
        #Initial connect
        self._connect()
    def _connect(self):
        """Connect or re-connect to AMQP server"""
        def on_connecterror(problem):
            """Problem connecting to AMQP server"""
            log.msg("Problem connecting to AMQP server " + str(problem), level=logging.ERROR)
            #Stop the application if we can't connect on a network level.
            self.running = False
            # Pylint Rational:
            #    Pylint and the Twisted reactor don't play nice, pylint fails to recognize
            #    reactor methods.
            #pylint: disable=no-member
            reactor.stop()
        def on_connect(connection):
            """Transport level connected. Set callback for application level connect to complete"""
            def reconect_in_five(argument=None):
                #Pylint Rational:
                #   reconect_in_five is a callback that should work with zero or one arguments.
                #   if called with one, the argument is ignored.
                #pylint: disable=unused-argument
                """Wait five seconds before reconnecting to server after connection lost"""
                log.msg("Reconnecting in five", logging.INFO)
                #Wait five seconds before we reconnect.
                task.deferLater(reactor, 5.0, self._connect)
            def on_connected(connection):
                """Handler that is called on connected on application level to AMQP server"""
                def on_channel(channel):
                    """Handler that gets called when the channel is ready"""
                    def exchange_declared():
                        """Handler that gets called when the exchange declaration is ready"""
                        #Start up 'window' get loops concurently to limit round trip latency
                        # becomming a prime limiting factor to performance.
                        for wnum in range(0, self.window):
                            # Pylint Rational:
                            #    Lazy Programmer; We need to loop self.window time,
                            #    wnum is not used.
                            #pylint: disable=unused-variable
                            self.hysteresis_queue.get(self._process_body)
                    self.channel = channel
                    channel.exchange_declare(
                        exchange=self.exchange,
                        durable=True,
                        auto_delete=False)
                    #Somehow adding a callback to exchange_declare does nothing.
                    #Instead of the callback, we wait half a second and assume that is enough.
                    task.deferLater(reactor, 0.5, exchange_declared)
                channel_deferred = connection.channel()
                channel_deferred.addCallback(on_channel)
            #If the connection gets closed by the server, reconnect in five
            connection.add_on_close_callback(reconect_in_five)
            #Set callback for when the application level connection is there
            connection.add_on_open_callback(on_connected)
        #Parameters containing login credentials
        parameters = ConnectionParameters(
            credentials=PlainCredentials(
                username=self.username,
                password=self.password))
        #Create a client using the rabbitmq login parameters.
        self.client = protocol.ClientCreator(
            reactor,
            TwistedProtocolConnection,
            parameters)
        #Connect the client to the server
        self.connect_deferred = self.client.connectTCP(self.host, self.port)
        #Set OK and fail callbacks
        self.connect_deferred.addCallback(on_connect)
        self.connect_deferred.addErrback(on_connecterror)
    def _process_body(self, body):
        def process_converted(converted_body):
            """Callback for body after convert"""
            def rmq_consume_error(problem):
                """Something went wrong with channel.basic_publish"""
                #pylint: disable=unused-argument
                self.running = False
                # Pylint Rational:
                #    Fixme: we should probably do something with the problem.
                #pylint: disable=no-member
                reactor.stop()
            def on_consumed(argument=None):
                """Called when channel.basic_publish completes"""
                # Pylint Rational:
                #    Callback that should work with one or zero arguments.
                #    Argument ignored.
                #pylint: disable=unused-argument
                self.hysteresis_queue.get(self._process_body)
            if isinstance(converted_body, (bytes, str)):
                props = BasicProperties(delivery_mode=2)
                self.publish_deferred = self.channel.basic_publish(
                    properties=props,
                    exchange=self.exchange,
                    routing_key=self.routing_key,
                    body=converted_body)
                self.publish_deferred.addCallbacks(on_consumed, rmq_consume_error)
            else:
                raise TypeError("converter should produce string or bytes, not",
                                type(converted_body))

        #We know this is a real broad exception catch clause, but as we need to be flexible
        #with regards to converters failing, we really do need to be this broad here.
        if self.running:
            try:
                self.converter(body, process_converted)
            #Pylint Rational:
            #    Converter is user code and could throw anything, we need a broad except here.
            #pylint: disable=broad-except
            except Exception as inst:
                log.msg("Error in converter:" + str(inst), logging.ERROR)
                #pylint: disable=no-member
                self.running = False
                reactor.stop()
