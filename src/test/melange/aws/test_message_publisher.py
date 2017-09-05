from unittest.mock import MagicMock

from melange.aws.event_serializer import EventSerializer
from melange.aws.message_publisher import MessagePublisher
from melange.aws.messaging_manager import MessagingManager
from melange.event import Event, EventSchema


class TestMessagePublisher:
    def test_publish_a_message(self):

        a_topic = 'a_topic'
        EventSerializer.instance().initialize({Event.event_type_name: EventSchema()})
        event = MagicMock(spec=Event)

        response = {'MessageId': '12345'}

        topic = MagicMock()
        topic.publish.return_value = response
        MessagingManager.declare_topic = MagicMock(return_value=topic)

        message_publisher = MessagePublisher(a_topic)
        success = message_publisher.publish(event)

        assert success
        topic.publish.assert_called()
