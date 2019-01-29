"""Pika RabitMQ consumer for thhqueue"""

try:
    from twisted.internet import protocol, reactor, task
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

class AioAmqpForwarder(object):
    #pylint: disable=too-few-public-methods
    #pylint: disable=too-many-instance-attributes
    """Asyncio based AmqpForwardConsumer using aio_pika library"""
    def __init__(self, hq, converter=None, host='localhost', port=5672, username="guest",
                 password="guest", exchange='foo', routing_key='quz', window=16):
        print("DEBUG 0")
        #pylint: disable=too-many-arguments
        def loopback(original, callback):
            """Standard do-nothing converter"""
            callback(original)
        if not HAS_AIO_PIKA:
            raise RuntimeError("Can not instantiate AioPikaConsumer because of failing imports.")
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
        self.running = True
        self._connect()
    def _connect(self):
        print("DEBUG 1 A")
        """Connect or re-connect to AMQP server"""
        def on_connect(connection_future):
            print("DEBUG 1 B")
            """Handler that gets called when the connection is made or has failed"""
            def on_channel(channel_future):
                print("DEBUG 1 C")
                """Handler that gets called when the channel is ready"""
                def on_exchange(exchange_future):
                    print("DEBUG 1 D")
                    """Handler that gets called when the exchange is ready or declaration failed"""
                    try:
                        print("DEBUG 1 D-a")
                        self.exchange = exchange_future.result()
                        print("DEBUG 1 D-b")
                    #pylint: disable=broad-except
                    except Exception as exception:
                        print("   + Problem defining exchange:", exception)
                        self.exchange = None
                        asyncio.get_event_loop().stop()
                    if self.exchange:
                        print("DEBUG 1 D-c")
                        #pylint: disable=unused-variable
                        for wnum in range(0, self.window):
                            print("DEBUG 1 D-d")
                            #pylint: disable=unused-variable
                            self.hysteresis_queue.get(self._process_body)
                            print("DEBUG 1 D-e")
                        print("DEBUG 1 D-f")
                    print("DEBUG 1 D-g")

                try:
                    print("DEBUG 1 C-a")
                    channel = channel_future.result()
                    print("DEBUG 1 C-b")
                #pylint: disable=broad-except
                except Exception as exception:
                    print("   + Problem regerstring channel:", exception)
                    channel = None
                    asyncio.get_event_loop().stop()
                print("DEBUG 1 C-c")
                if channel:
                    print("DEBUG 1 C-d")
                    amqp_exchange_future = asyncio.ensure_future(
                        channel.declare_exchange(
                            name=self.exchange,
                            durable=True))
                            #type=aio_pika.ExchangeType.HEADERS))
                    print("DEBUG 1 C-e")
                    amqp_exchange_future.add_done_callback(on_exchange)
                    print("DEBUG 1 C-f")
            try:
                print("DEBUG 1 B-a")
                connection = connection_future.result()
                print("DEBUG 1 B-b")
            #pylint: disable=broad-except
            except Exception as exception:
                print("   + Problem connecting:", exception)
                connection = None
                asyncio.get_event_loop().stop()
            print("DEBUG 1 B-c")
            if connection:
                print("DEBUG 1 B-d")
                amqp_channel_future = asyncio.ensure_future(
                    connection.channel())
                print("DEBUG 1 B-e")
                amqp_channel_future.add_done_callback(on_channel)
                print("DEBUG 1 B-f")
        print("DEBUG 1 A-a")
        amqp_connection_future = asyncio.ensure_future(
            aio_pika.connect_robust(
                host=self.host,
                port=self.port,
                login=self.username,
                password=self.password,
                loop=asyncio.get_event_loop()))
        print("DEBUG 1 A-b")
        amqp_connection_future.add_done_callback(on_connect)
        print("DEBUG 1 A-c")
    def _process_body(self, body_future):
        print("DEBUG 2 A")
        def process_converted(converted_body):
            print("DEBUG 2 B")
            """Callback for body after convert"""
            def on_published(done_future):
                print("DEBUG 2 C")
                """Callback for publish ready or failed"""
                try:
                    done_future.result()
                    self.hysteresis_queue.get(self._process_body)
                #pylint: disable=broad-except
                except Exception as exception:
                    print("   + Problem publishing:", exception)
                    self.running = False
                    asyncio.get_event_loop().stop()
            print("DEBUG 2 B-a")
            if isinstance(converted_body, (bytes, str)):
                print("DEBUG 2 B-b")
                publish_done_future = self.exchange.publish(
                    aio_pika.Message(converted_body),
                    routing_key=self.routing_key)
                print("DEBUG 2 B-c")
                publish_done_future.add_done_callback(on_published)
                print("DEBUG 2 B-d")
            else:
                print("DEBUG 2 B-e")
                raise TypeError("converter should produce string or bytes, not",
                                type(converted_body))
            print("DEBUG 2 B-f")
        print("DEBUG 2 A-a")
        body = body_future.result()
        print("DEBUG 2 A-b")
        if self.running:
            try:
                print("DEBUG 2 A-c")
                self.converter(body, process_converted)
                print("DEBUG 2 A-d")
            #pylint: disable=broad-except
            except Exception as inst:
                print("Converter error:", inst)
                print(type(self.converter), self.converter)
                print(body)
                self.running = False
                asyncio.get_event_loop().stop()

class TxAmqpForwarder(object):
    """Twisted based AmqpForwardConsumer using pica TwistedProtocolConnection"""
    #Seems like a decent idea to fix the below, later.
    #pylint: disable=too-many-instance-attributes
    #pylint: disable=too-few-public-methods
    def __init__(self, hq, converter=None, host='localhost', port=5672, username="guest",
                 password="guest", exchange='foo', routing_key='quz', window=16):
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
            print("   + Problem connecting:", problem.value)
            #Stop the application if we can't connect on a network level.
            #pylint: disable=no-member
            self.running = False
            reactor.stop()
        def on_connect(connection):
            """Transport level connected. Set callback for application level connect to complete"""
            def reconect_in_five(argument=None):
                """Wait five seconds before reconnecting to server after connection lost"""
                print("     + Reconnecting in five:", argument)
                #Wait five seconds before we reconnect.
                task.deferLater(reactor, 5.0, self._connect)
            def on_connected(connection):
                """Handler that is called on connected on application level to AMQP server"""
                def on_channel(channel):
                    """Handler that gets called when the channel is ready"""
                    def exchange_declared():
                        """Handler that gets called when the exchange declaration is ready"""
                        #pylint: disable=unused-variable
                        #Start up 'window' get loops concurently to limit round trip latency
                        # becomming a prime limiting factor to performance.
                        for wnum in range(0, self.window):
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
                #pylint: disable=no-member
                self.running = False
                reactor.stop()
            def on_consumed(argument=None):
                """Called when channel.basic_publish completes"""
                #pylint: disable=unused-argument
                self.hysteresis_queue.get(self._process_body)
            if isinstance(converted_body, (bytes, str)):
                #pylint: disable=no-member
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
        #pylint: disable=broad-except
        if self.running:
            try:
                self.converter(body, process_converted)
            #pylint: disable=broad-except
            except Exception as inst:
                print(inst)
                #pylint: disable=no-member
                self.running = False
                reactor.stop()
