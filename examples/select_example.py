import time

import utils.secret_code as secret_code
import message_queueing.select_connection as select_connection


def handle_message(body_str):
    print(f"Message {body_str} received.")


def main():
    cloud_amqp_interface = select_connection.MqSelectConnectionInterface(
        mq_broker_url=USE_AMQP_URL,
        queue_name='select_basic',
        routing_key="inc.telemetry.*",
        on_message_callback=handle_message,
        exchange="my_topic_router",
        exchange_type="topic"
    )

    cloud_amqp_interface.run()

    while True:
        secret_code.display_message()
        time.sleep(5)


if __name__ == "__main__":
    main()
