import asyncio
import aio_pika

import utils.secret_code as secret_code


def handle_message(body_str):
    print(f"Message {body_str} received.")


async def subscribe(loop):
    connection = await aio_pika.connect_robust(
        USE_AMQP_URL,
        loop=loop
    )

    queue_name = "basic_async"
    exchange = "my_topic_router"

    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, auto_delete=True)
        await queue.bind(exchange, "inc.telemetry.*")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    handle_message(body_str=message.body)

                    if queue.name in message.body.decode():
                        break


async def show_secret_message():
    while True:
        secret_code.display_message()
        await asyncio.sleep(7)


async def main():
    loop = asyncio.get_event_loop()
    await asyncio.gather(show_secret_message(), subscribe(loop))

if __name__ == "__main__":
    asyncio.run(main())
