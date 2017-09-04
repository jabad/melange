from melange.event import Event, EventSchema
from melange.infrastructure.singleton import Singleton


@Singleton
class EventSerializer:
    def __init__(self):
        schemas = {}
        self.event_serializer_map = {}

        for schema in EventSchema.__subclasses__():
            schemas[schema.__qualname__] = schema

        for event in Event.events:
            self.event_serializer_map[event.event_type_name] = schemas['{}Schema'.format(event.__qualname__)]

    def deserialize(self, event_dict):
        event_type_name = event_dict['event_type_name']

        if event_type_name not in self.event_serializer_map:
            raise ValueError("The event type {} doesn't have a registered serializer")

        schema = self.event_serializer_map[event_type_name]
        data, errors = schema().load(event_dict)
        return data

    def serialize(self, event):

        event_type_name = event.event_type_name

        if event_type_name not in self.event_serializer_map:
            raise ValueError("The event type {} doesn't have a registered serializer")

        schema = self.event_serializer_map[event_type_name]
        return schema().dumps(event).data
