import asyncio
import typing
from urllib.parse import parse_qs, urlparse

from google.cloud import pubsub_v1

from ..._base import BackendSchemes, Event
from ..base import BroadcastBackend


class GoogleCloudPubSubBackend(BroadcastBackend):
    def __init__(self, url: str):
        self._channel_index = 0
        self._should_stop = False
        self._topics: typing.Dict = {}
        self._subscriptions: typing.Dict = {}

        parsed_url = urlparse(
            url=url,
            scheme=BackendSchemes.google_cloud_pubsub.value,
        )

        self.project = parsed_url.netloc

        query_params = parse_qs(parsed_url.query)
        consumer_wait_time_param = "consumer_wait_time"
        self._subscriber_wait_time = (
            float(query_params[consumer_wait_time_param][0])
            if consumer_wait_time_param in query_params
            else 1.0
        )

    def _create_topic(self, channel: str) -> str:
        topic = self._topics.get(channel)

        if not topic:
            topic = self._publisher.topic_path(
                project=self.project,
                topic=channel,
            )
            self._publisher.create_topic(
                name=topic,
            )
            self._topics[channel] = topic

        return topic

    async def connect(self) -> None:
        self._publisher = pubsub_v1.PublisherClient()
        self._subscriber = pubsub_v1.SubscriberClient()

    async def disconnect(self) -> None:
        self._should_stop = True
        self._publisher = None
        self._subscriber = None

    async def subscribe(self, channel: str) -> None:
        if self._subscriptions.get(channel):
            return

        subscription = self._subscriber.subscription_path(
            project=self.project,
            subscription=channel,
        )
        topic = self._create_topic(
            channel=channel,
        )
        self._subscriber.create_subscription(
            name=subscription,
            topic=topic,
        )
        self._subscriptions[channel] = subscription

    async def unsubscribe(self, channel: str) -> None:
        self._subscriptions.pop(channel)

    async def publish(self, channel: str, message: typing.Any) -> None:
        topic = self._create_topic(
            channel=channel,
        )
        self._publisher.publish(
            topic=topic,
            data=message.encode(),
        )

    async def next_published(self) -> Event:
        while not self._should_stop:
            subscriptions = list(self._subscriptions.items())

            if not subscriptions:
                await asyncio.sleep(
                    delay=self._subscriber_wait_time,
                )
                continue

            channel_id, subscription = subscriptions[self._channel_index]

            response = self._subscriber.pull(
                subscription=subscription,
                return_immediately=True,
                max_messages=1,
            )

            if not response.received_messages:
                await asyncio.sleep(
                    delay=self._subscriber_wait_time,
                )
                self._channel_index = (
                    0
                    if self._channel_index >= len(subscriptions) - 1
                    else self._channel_index + 1
                )
            else:
                response = response.received_messages[0]
                event = Event(
                    channel=channel_id,
                    message=response.message.data.decode(),
                )
                self._subscriber.acknowledge(
                    subscription=subscription,
                    ack_ids=[response.ack_id],
                )
                return event

        raise RuntimeError(
            "Couldn't get the next published event for"
            f" {BackendSchemes.google_cloud_pubsub.value}"
        )
