"""Pika RabitMQ consumer for thhqueue"""

try:
    from twisted.internet import protocol, reactor, task
    from pika.adapters.twisted_connection import TwistedProtocolConnection
    from pika import ConnectionParameters,PlainCredentials
    HAS_TWISTED_PIKA=True
except:
    HAS_TWISTED_PIKA=False

class TxAmqpForwarder(object):
    """Twisted based AmqpForwardConsumer using pica TwistedProtocolConnection"""
    def __init__(self, hq, converter=None, host='localhost', port=5672, username="guest",
            password="guest", exchange='foo', routing_key='quz'):
        if not HAS_TWISTED_PIKA:
            raise RuntimeError("Can not instantiate TxPikaConsumer because of failing imports.")
        #Remember hysteresis queue we need to be consuming from
        self.hq=hq
        #Set the conversion callback.
        if converter is None:
            #If none is specified, set it to a do nothing loopback
            self.converter = self.loopback
        else:
            #Otherwise set the converter as specified in the constructor
            self.converter = converter
        #Server and port
        self.host=host
        self.port=port
        #User credentials
        self.username=username
        self.password=password
        #AMQP settings
        self.exchange = exchange
        self.routing_key = routing_key
        #Set members used later to None
        self.client = None
        self.connect_deferred = None
        self.publish_deferred = None
        self.channel = None
        #Initial connect
        self.connect()
    def connect(self):
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
        self.connect_deferred.addCallback(self.on_connect)
        self.connect_deferred.addErrback(self.on_connecterror)
    def on_connect(self, connection):
        #If the connection gets closed by the server, reconnect in five
        connection.add_on_close_callback(self.reconect_in_five)
        #Set callback for when the application level connection is there
        connection.add_on_open_callback(self.on_connected)
    def on_connecterror(self,problem):
        print("   + Problem connecting:",problem.value)
        #Stop the application if we can't connect on a network level.
        reactor.stop()
    def reconect_in_five(self,to=0):
        print("     + Reconnecting in five:", to)
        #Wait five seconds before we reconnect. 
        task.deferLater(reactor, 5.0, self.connect)
    def on_connected(self,connection):
        cd = connection.channel()
        cd.addCallback(self.on_channel)
    def on_channel(self,channel):
        self.channel = channel
        #FIXME: Something goes wrong here if we add the callback. It never gets called.
        channel.exchange_declare(
            exchange=self.exchange,
            durable=True,
            auto_delete=False)
        #So instead of the callback, we wait one second and assume that is enough.
        task.deferLater(reactor, 1.0, self.exchange_declared)
    def exchange_declared(self):
        self.hq.get(self.process_body)
    def loopback(self, original):
        return original
    def process_body(self,body):
        try:
            converted_body = self.converter(body)
        except Exception as inst:
            print(inst)
            converted_body = None
        if converted_body:
            self.publish_deferred = self.channel.basic_publish(
                exchange=self.exchange,
                routing_key=self.routing_key,
                body=body)
            self.publish_deferred.addCallbacks(self.on_consumed,self.rmq_consume_error)
        else:
            reactor.stop()
    def on_consumed(self,to=0):
        self.hq.get(self.process_body)
    def rmq_consume_error(self,problem):
        reactor.stop()


