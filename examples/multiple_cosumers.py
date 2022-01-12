import asyncio
import aio_pika

import utils.secret_code as secret_code


def handle_message1(body_str, queue_name):
    print(f"Message {body_str} received on {queue_name}. Pretend to to something in handler1.")


def handle_message2(body_str, queue_name):
    print(f"Message {body_str} received on {queue_name}. handler 2")


def handle_message3(body_str, queue_name):
    print(f"Message {body_str} received on {queue_name}. handler 3")


async def subscribe(connection, queue_name, exchange, callback, routing_key):
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, auto_delete=True)
        await queue.bind(exchange, routing_key)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    callback(body_str=message.body, queue_name=queue_name)

                    if queue.name in message.body.decode():
                        break


async def show_secret_message():
    while True:
        secret_code.display_message()
        await asyncio.sleep(7)


async def main():
    loop = asyncio.get_event_loop()
    connection = await aio_pika.connect_robust(
        USE_AMQP_URL,
        loop=loop,
    )

    queue_name = "basic_async"
    exchange = "my_topic_router"

    await asyncio.gather(
            subscribe(
                connection=connection,
                queue_name=f"{queue_name}{1}",
                exchange=exchange,
                callback=handle_message1,
                routing_key="inc.telemetry.*"
            ),
            subscribe(
                connection=connection,
                queue_name=f"{queue_name}{2}",
                exchange=exchange,
                callback=handle_message2,
                routing_key="inc.config.*"
            ),
            subscribe(
                connection=connection,
                queue_name=f"{queue_name}{3}",
                exchange=exchange,
                callback=handle_message3,
                routing_key="vw.telemetry.*"
            )
    )


if __name__ == "__main__":
    asyncio.run(main())
