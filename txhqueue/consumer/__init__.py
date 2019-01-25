"""Pika RabitMQ consumer for thhqueue"""

try:
    from twisted.internet import protocol, reactor, task
    from pika.adapters.twisted_connection import TwistedProtocolConnection
    from pika import ConnectionParameters, PlainCredentials, BasicProperties
    HAS_TWISTED_PIKA = True
except ImportError:
    HAS_TWISTED_PIKA = False

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
            if isinstance(converted_body,bytes) or isinstance(converted_body,bytes):
                #pylint: disable=no-member
                props = BasicProperties(delivery_mode=2)
                self.publish_deferred = self.channel.basic_publish(
                    properties=props,
                    exchange=self.exchange,
                    routing_key=self.routing_key,
                    body=converted_body)
                self.publish_deferred.addCallbacks(on_consumed, rmq_consume_error)
            else:
                raise TypeError("converter should produce string or bytes")
        #We know this is a real broad exception catch clause, but as we need to be flexible
        #with regards to converters failing, we really do need to be this broad here.
        #pylint: disable=broad-excepta
        if self.running:
            try:
                self.converter(body, process_converted)
            except Exception as inst:
                print(inst)
                #pylint: disable=no-member
                self.running = False
                reactor.stop()
