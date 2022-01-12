import asyncio
import random

import aio_pika
import json


async def generate_and_publish(period: int, node_id: str, node_type: str, message_type, loop):
    connection = await aio_pika.connect_robust(
        USE_AMQP_URL,
        loop=loop
    )
    frm_cnt = 0
    async with connection:
        routing_key = f"{node_type}.{message_type}.{node_id}"

        channel = await connection.channel()
        exchange = await channel.declare_exchange(name="my_topic_router",
                                                  type=aio_pika.ExchangeType.TOPIC, durable=True)
        while True:
            body_str = json.dumps(
                {"node_id": node_id, "node_type": node_type, "message_type": message_type,
                 "frm_cnt": frm_cnt})
            frm_cnt += 1
            await asyncio.sleep(random.randint(1, 3))
            print(f"Sending {body_str}.")
            await exchange.publish(
                aio_pika.Message(body=body_str.encode()),
                routing_key=routing_key,
            )
            await asyncio.sleep(period)


async def main():
    loop = asyncio.get_event_loop()
    await asyncio.gather(generate_and_publish(period=10, node_id="ls-1", node_type="inc",
                                              message_type="telemetry", loop=loop),
                         generate_and_publish(period=10, node_id="ls-2", node_type="vw",
                                              message_type="telemetry", loop=loop),
                         generate_and_publish(period=10, node_id="ls-3", node_type="til90",
                                              message_type="telemetry", loop=loop),
                         generate_and_publish(period=60, node_id="ls-3", node_type="til90",
                                              message_type="config", loop=loop)
                         )

if __name__ == "__main__":
    asyncio.run(main())
