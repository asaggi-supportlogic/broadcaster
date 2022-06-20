import asyncio
from uuid import uuid4

import pytest

from broadcaster._backends.redis import RedisBackend

from .conftest import URLS

_URLS = list(URLS.values()) + [
    ("google-cloud-pubsub://broadcaster-local?consumer_wait_time=0.1",)
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ["setup_broadcast"],
    _URLS,
    indirect=True,
)
async def test_broadcast(setup_broadcast):
    guid = uuid4()
    channel = f"chatroom-{guid}"
    message = f"hello {guid}"
    async with setup_broadcast.subscribe(channel) as subscriber:
        # [FIXME]:
        #     There's likely a race condition in Redis where the publish happens before
        #     the subscribe and is temporarily worked around by an asynchronous sleep
        #     (see: https://github.com/encode/broadcaster/issues/44).
        if isinstance(setup_broadcast._backend, RedisBackend):
            await asyncio.sleep(0.1)
        await setup_broadcast.publish(channel, message)
        event = await subscriber.get()
        assert event.channel == channel
        assert event.message == message
