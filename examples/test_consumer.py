import json
import asyncio
import time

import aio_pika


def handle_message(body_str):
    print(f"Message received: ")
    print(json.loads(body_str))


async def subscribe(loop, url, routing_key, queue_name, exchange="amq.topic"):
    connection = await aio_pika.connect_robust(
        url,
        loop=loop
    )

    async with connection:
        channel = await connection.channel()
        my_queue = f"{queue_name}{time.time()}"
        queue = await channel.declare_queue(my_queue, auto_delete=True, exclusive=True)
        await queue.bind(exchange, routing_key)
        print("Waiting for messages.")
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    handle_message(body_str=message.body)


async def main(amqp_url, routing_key, queue_name):
    loop = asyncio.get_event_loop()
    await asyncio.gather(subscribe(loop,
                                   url=amqp_url,
                                   routing_key=routing_key,
                                   queue_name=queue_name))


if __name__ == "__main__":
    url = USE_AMQP_URL

    asyncio.run(main(amqp_url=url,
                     routing_key="demo23605",
                     queue_name="temp_queue_console"))
