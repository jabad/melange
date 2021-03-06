import json
import uuid
from unittest.mock import MagicMock

from melange.messaging import DriverManager, Message
from melange.messaging import MessagingDriver
from melange.messaging.event_serializer import EventSerializer
from melange.messaging.event_message import EventMessage
from melange.messaging.exchange_listener import ExchangeListener
from melange.messaging.exchange_message_consumer import ExchangeMessageConsumer


class TestMessageConsumer:
    def test_consume_on_empty_queue(self):
        event_type_name = 'an_event_type_event'

        self._Scenario() \
            .given_an_empty_queue_to_listen() \
            .given_a_subscriber(listens_to=event_type_name) \
            .when_consuming_the_next_event_in_queue() \
            .then_the_subscriber_has_not_processed_any_event()

    def test_consume_on_queue_with_message(self):
        event_type_name = 'an_event_type_event'

        self._Scenario() \
            .given_a_queue_to_listen_with_an_event(event_of_type=event_type_name) \
            .given_a_subscriber(listens_to=event_type_name) \
            .when_consuming_the_next_event_in_queue() \
            .then_the_subscriber_has_processed_the_event() \
            .the_message_has_been_acknowledged()

    def test_consume_on_queue_with_message_but_subscriber_listens_to_different_events(self):
        event_type_name = 'an_event_type_event'
        event_type_name_2 = 'an_event_type_event_2'

        self._Scenario() \
            .given_a_queue_to_listen_with_an_event(event_of_type=event_type_name) \
            .given_a_subscriber(listens_to=event_type_name_2) \
            .when_consuming_the_next_event_in_queue() \
            .then_the_subscriber_has_not_processed_any_event() \
            .the_message_has_been_acknowledged()

    def test_unsubscribe(self):
        event_type_name = 'an_event_type_event'

        self._Scenario() \
            .given_a_queue_to_listen_with_an_event(event_of_type=event_type_name) \
            .given_a_subscriber(listens_to=event_type_name) \
            .when_unsubscribing_the_subscriber() \
            .when_consuming_the_next_event_in_queue() \
            .then_the_subscriber_has_not_processed_any_event() \
            .the_message_has_been_acknowledged()

    class _Scenario:
        def __init__(self):
            self.message_consumer = None
            self.subscriber = None
            self.event = None
            self.messages = None
            self.driver = MagicMock(spec=MessagingDriver)
            DriverManager.instance().use_driver(driver=self.driver)

        def given_a_queue_to_listen_with_an_event(self, event_of_type=None):
            self.messages = [self._create_message(i) for i in range(1)]
            queue = self._create_queue(self.messages)
            self.driver.declare_queue.return_value = (queue, '')

            self.event = Message(uuid.uuid4(), {'event_type_name': event_of_type}, None)
            self.driver.retrieve_messages.return_value = [self.event]

            event_queue_name = 'a_queue_name'
            topic_to_subscribe = 'a_topic_name'
            self.message_consumer = ExchangeMessageConsumer(event_queue_name, topic_to_subscribe)

            return self

        def given_a_subscriber(self, listens_to=None):
            self.subscriber = MagicMock(spec=ExchangeListener)
            self.subscriber.listens_to.return_value = listens_to

            self.message_consumer.subscribe(self.subscriber)

            return self

        def when_consuming_the_next_event_in_queue(self):
            self.message_consumer.consume_event()

            return self

        def then_the_subscriber_has_processed_the_event(self):
            self.subscriber.process_event.assert_called()
            return self

        def the_message_has_been_acknowledged(self):
            self.driver.acknowledge.assert_called()

            return self

        def given_an_empty_queue_to_listen(self):
            queue = self._create_queue()
            self.driver.declare_queue = MagicMock(return_value=(queue, ''))
            self.driver.declare_topic = MagicMock()

            event_queue_name = 'a_queue_name'
            topic_to_subscribe = 'a_topic_name'
            self.message_consumer = ExchangeMessageConsumer(event_queue_name, topic_to_subscribe)

            return self

        def then_the_subscriber_has_not_processed_any_event(self):
            self.subscriber.process.assert_not_called()
            return self

        def _create_queue(self, messages=[]):
            queue = MagicMock()

            self.driver.retrieve_messages.return_value = messages

            return queue

        def _create_message(self, i):
            message = MagicMock()
            message.body = json.dumps({
                'event_type_name': 'an_event_name'
            })
            return message

        def when_unsubscribing_the_subscriber(self):
            self.message_consumer.unsubscribe(self.subscriber)
            return self
