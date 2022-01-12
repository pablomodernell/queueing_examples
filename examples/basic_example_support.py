import time

import utils.secret_code as secret_code
import message_queueing.basic_blocking as basic_blocking


def handle_message(body_str):
    print(f"Message {body_str} received.")


def message_callback(ch, method, properties, body):
    handle_message(body_str=body.decode())


def main():
    cloud_amqp_interface = basic_blocking.MqInterface(
        mq_broker_url=USE_AMQP_URL
    )
    queue_name = 'basic_blocking'
    exchange = "my_topic_router"

    cloud_amqp_interface.declare_exchange(exchange_name=exchange, exchange_type="topic")

    cloud_amqp_interface.declare_queue(queue_name=queue_name, auto_delete=False,
                                       exclusive=False)

    cloud_amqp_interface.bind_queue(queue_name=queue_name,
                                    routing_key='inc.telemetry.*',
                                    exchange_name=exchange)

    consumer_tag = cloud_amqp_interface.create_consumer(
        callback=message_callback, queue_name=queue_name, auto_ack=True
    )

    cloud_amqp_interface.channel.start_consuming()

    while True:
        secret_code.display_message()
        time.sleep(5)


if __name__ == "__main__":
    main()
