import message_queueing.basic_blocking as basic_blocking


def handle_message_inc(ch, method, properties, body):
    print(f"Message {body} received on {ch} channel.")


if __name__ == "__main__":
    cloud_amqp_interface = basic_blocking.MqInterface(
        mq_broker_url=USE_AMQP_URL
    )
    queue_name = 'inc_for_basic'
    cloud_amqp_interface.declare_queue(queue_name=queue_name, auto_delete=False,
                                       exclusive=False)
    cloud_amqp_interface.bind_queue(queue_name=queue_name,
                                    routing_key='inc.telemetry.*')
    consumer_tag = cloud_amqp_interface.create_consumer(
        callback=handle_message_inc, queue_name=queue_name, auto_ack=True
    )
    cloud_amqp_interface.channel.start_consuming()
