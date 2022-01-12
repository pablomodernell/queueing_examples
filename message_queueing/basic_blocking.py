import pika


class MqInterface(object):
    """Wrapper class for interfacing with the message broker."""

    def __init__(self, mq_broker_url):
        """
        The lorawan_parameters for the connection with the RMQ Broker must be in an
        environment variable *AMQP_URL*.
        """
        self.amqp_url = mq_broker_url
        self.params = pika.URLParameters(self.amqp_url)
        self._connection = pika.BlockingConnection(self.params)
        self._channel = self.connection.channel()
        self._knownQueues = []
        self._knownExchanges = []

    @property
    def connection(self):
        if not (self._connection and self._connection.is_open):
            self._connection = pika.BlockingConnection(self.params)
        return self._connection

    @property
    def channel(self):
        if self._channel.is_closed:
            self._channel = self.connection.channel()
        return self._channel

    def declare_queue(
            self, queue_name, exclusive=True, auto_delete=False, durable=True
    ):
        """
        Declares a new queue in the message broker.
        :param queue_name:
        :param exclusive:
        :param auto_delete:
        :return: queue result
        """
        queue_result = self.channel.queue_declare(
            queue=queue_name,
            exclusive=exclusive,
            auto_delete=auto_delete,
            durable=durable,
        )
        if queue_name not in self._knownQueues:
            self._knownQueues.append(queue_name)
        return queue_result

    def declare_exchange(self, exchange_name, exchange_type='topic'):
        self.channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type,
                                      durable=True)

    def bind_queue(self, queue_name, routing_key, exchange_name):
        self.channel.queue_bind(
            queue=queue_name, exchange=exchange_name, routing_key=routing_key
        )

    def create_consumer(self, callback, queue_name, tag="", auto_ack=True):
        return self.channel.basic_consume(
            on_message_callback=callback,
            queue=queue_name,
            auto_ack=auto_ack,
            consumer_tag=tag,
        )

    def declare_and_consume(
            self,
            exchange,
            queue_name,
            routing_key,
            callback,
            exclusive=True,
            auto_delete=False,
            auto_ack=True,
            durable=True,
    ):
        queue_result = self.declare_queue(
            queue_name, exclusive=exclusive, auto_delete=auto_delete, durable=durable
        )
        self.bind_queue(
            exchange_name=exchange,
            queue_name=queue_name,
            routing_key=routing_key,
        )
        consumer_tag = self.create_consumer(
            callback=callback, queue_name=queue_name, auto_ack=auto_ack
        )
        return queue_result, consumer_tag

    def publish(self, msg, routing_key, exchange_name):
        self.channel.basic_publish(
            exchange=exchange_name, routing_key=routing_key, body=msg
        )

    def consume_start(self):
        """
        Start consuming messages.
        :return:
        """
        self.channel.start_consuming()

    def consume_stop(self):
        """
        Cancels all consumers.
        :return:
        """
        self.channel.stop_consuming()
