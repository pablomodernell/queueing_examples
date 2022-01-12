import pika
import pika.exceptions

import logging

logger = logging.getLogger(__name__)


class MqSelectConnectionInterface(object):
    def __init__(
        self,
        mq_broker_url,
        queue_name,
        routing_key,
        on_message_callback,
        exchange='amq.topic',
        exchange_type="topic",
        queue_durable=True,
        queue_exclusive=False,
        queue_auto_delete=False,
    ):
        self.amqp_url = mq_broker_url
        self._url = self.amqp_url

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None

        self.exchange = exchange
        self.exchange_type = "topic"
        self.queue = queue_name
        self.queue_durable = queue_durable
        self.queue_exclusive = queue_exclusive
        self.queue_auto_delete = queue_auto_delete
        self.routing_key = routing_key
        self.on_message_callback = on_message_callback

    def connect(self):
        logger.info("Connecting to %s", self._url)
        return pika.SelectConnection(
            pika.URLParameters(self._url),
            self.on_connection_open,
        )

    def on_connection_open(self, connection):
        logger.info("Connection opened.")
        self._connection.add_on_close_callback(self.on_connection_closed)
        self.open_channel()

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            logger.warning("Connection closed(%s) %s", reply_code, reply_text)

    def open_channel(self):
        logger.debug("Creating a new channel")
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        logger.info("Channel opened")
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self.setup_exchange(self.exchange)

    def on_channel_closed(self, channel, exception):
        logger.warning("Channel %i was closed: %s", channel, exception)
        logger.exception(exception)
        self._connection.close()

    def setup_exchange(self, exchange_name):
        logger.debug("Declaring exchange %s", exchange_name)
        self._channel.exchange_declare(
            exchange_name,
            callback=self.on_exchange_declareok,
            exchange_type=self.exchange_type,
            durable=True,
        )

    def on_exchange_declareok(self, frame):
        logger.debug("Exchange declared")

        logger.debug("Declaring queue %s", self.queue)
        self._channel.queue_declare(
            self.queue,
            callback=self.on_queue_declareok,
            durable=self.queue_durable,
            exclusive=self.queue_exclusive,
            auto_delete=self.queue_auto_delete,
        )

    def on_queue_declareok(self, method_frame):
        logger.debug(
            "Binding %s to %s with %s", self.exchange, self.queue, self.routing_key
        )
        self._channel.queue_bind(
            self.queue,
            self.exchange,
            routing_key=self.routing_key,
            callback=self.on_bindok,
        )

    def on_bindok(self, frame):
        logger.debug("Queue bound")
        self.start_consuming()

    def start_consuming(self):
        self._consumer_tag = self._channel.basic_consume(self.queue, self.on_message, auto_ack=True)

    def stop_consuming(self):
        if self._channel:
            logger.debug("Sending a Basic.Cancel RPC command to RabbitMQ")
            self._channel.basic_cancel(
                callback=self.on_cancelok, consumer_tag=self._consumer_tag
            )

    def on_cancelok(self, frame):
        logger.debug("RabbitMQ acknowledged the cancellation of the consumer")
        logger.debug("Closing the channel")
        self._channel.close()

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        logger.debug("Stopping")
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        logger.debug("Stopped")

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        logger.debug("Closing connection")
        self._connection.close()

    def on_message(self, channel, basic_deliver, properties, body):
        body_str = body.decode()
        logger.info(f"Message received {body_str}.")
        self.on_message_callback(body_str=body_str)

    def consume_start(self):
        """
        Start consuming messages.
        :return:
        """
        self.run()
