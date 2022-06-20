import asyncio
from uuid import uuid4

import pytest

from broadcaster._backends.redis import RedisBackend

from .conftest import URLS

_MESSAGES = ["hello", "goodbye"]


@pytest.mark.asyncio
@pytest.mark.parametrize(["setup_broadcast"], URLS.values(), indirect=True)
async def test_broadcast(setup_broadcast):
    uid = uuid4()
    channel = f"chatroom-{uid}"
    msgs = [f"{msg} {uid}" for msg in _MESSAGES]
    async with setup_broadcast.subscribe(channel) as subscriber:
        # [FIXME]:
        #     There's likely a race condition in Redis where the publish happens before
        #     the subscribe and is temporarily worked around by an asynchronous sleep
        #     (see: https://github.com/encode/broadcaster/issues/44).
        if isinstance(setup_broadcast._backend, RedisBackend):
            await asyncio.sleep(0.1)
        to_publish = [setup_broadcast.publish(channel, msg) for msg in msgs]

        await asyncio.gather(*to_publish)
        for msg in msgs:
            event = await subscriber.get()
            assert event.channel == channel
            assert event.message == msg


@pytest.mark.asyncio
@pytest.mark.parametrize(["setup_broadcast"], URLS.values(), indirect=True)
async def test_sub(setup_broadcast):
    uid = uuid4()
    channel1 = f"chatroom-{uid}1"
    channel2 = f"chatroom-{uid}2"

    to_sub = [
        setup_broadcast._backend.subscribe(channel1),
        setup_broadcast._backend.subscribe(channel2),
    ]
    await asyncio.gather(*to_sub)


@pytest.mark.asyncio
@pytest.mark.parametrize(["setup_broadcast"], URLS.values(), indirect=True)
async def test_unsub(setup_broadcast):
    uid = uuid4()
    channel1 = f"chatroom-{uid}1"
    channel2 = f"chatroom-{uid}2"

    await setup_broadcast._backend.subscribe(channel1)
    await setup_broadcast._backend.subscribe(channel2)

    to_unsub = [
        setup_broadcast._backend.unsubscribe(channel1),
        setup_broadcast._backend.unsubscribe(channel2),
    ]

    await asyncio.gather(*to_unsub)
