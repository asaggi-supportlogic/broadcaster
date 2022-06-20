import asyncio
import functools
import os

import pytest_asyncio

from broadcaster import Broadcast
from broadcaster._backends.google.pubsub import GoogleCloudPubSubBackend
from broadcaster._backends.kafka import KafkaBackend
from broadcaster._backends.memory import MemoryBackend
from broadcaster._backends.postgres import PostgresBackend
from broadcaster._backends.redis import RedisBackend

URLS = {
    MemoryBackend: ("memory://",),
    RedisBackend: ("redis://localhost:6379",),
    PostgresBackend: ("postgres://postgres:postgres@localhost:5432/broadcaster",),
    KafkaBackend: ("kafka://localhost:9092",),
    GoogleCloudPubSubBackend: ("google-cloud-pubsub://broadcaster-local",),
}


async def __has_topic_now(client, topic):
    if await client.force_metadata_update():
        if topic in client.cluster.topics():
            print(f'Topic "{topic}" exists')
            return True
    return False


async def __wait_has_topic(client, topic, *, timeout_sec=5):
    poll_time_sec = 1 / 10000
    from datetime import datetime

    pre = datetime.now()
    while True:
        if (datetime.now() - pre).total_seconds() >= timeout_sec:
            raise ValueError(f'No topic "{topic}" exists')
        if await __has_topic_now(
            client=client,
            topic=topic,
        ):
            return
        await asyncio.sleep(
            delay=poll_time_sec,
        )


def kafka_backend_setup(kafka_backend):
    subscribe_impl = kafka_backend.subscribe

    @functools.wraps(
        wrapped=subscribe_impl,
    )
    async def subscribe(channel: str) -> None:
        await subscribe_impl(channel)
        await __wait_has_topic(
            client=kafka_backend._consumer._client,
            topic=channel,
        )

    kafka_backend.subscribe = subscribe


BROADCAST_SETUPS = {
    KafkaBackend: kafka_backend_setup,
}


def google_cloud_pubsub_environ():
    os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8086"


ENVIRON_SETUPS = {
    google_cloud_pubsub_environ,
}


@pytest_asyncio.fixture(scope="function")
async def setup_broadcast(request):
    url = request.param

    for environ in ENVIRON_SETUPS:
        environ()

    async with Broadcast(url) as broadcast:
        backend = broadcast._backend
        for cls, setup in BROADCAST_SETUPS.items():
            if isinstance(backend, cls):
                setup(backend)
                break
        yield broadcast
