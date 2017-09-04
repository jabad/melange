from abc import ABCMeta, abstractmethod
from datetime import datetime

from marshmallow import Schema, fields
from marshmallow.schema import SchemaMeta


class EventSchemaMeta(SchemaMeta):
    def __init__(metaclass, name, bases, attrs):
        super().__init__(name, bases, attrs)
        if not hasattr(metaclass, 'schemas'):
            # This branch only executes when processing the mount point itself.
            # So, since this is a new plugin type, not an implementation, this
            # class shouldn't be registered as a plugin. Instead, it sets up a
            # list where plugins can be registered later.
            metaclass.schemas = []
        else:
            # This must be a plugin implementation, which should be registered.
            # Simply appending it to the list is all that's needed to keep
            # track of it later.
            metaclass.schemas.append(metaclass)

class EventSchema(Schema, metaclass=EventSchemaMeta):
    event_type_name = fields.Str()
    event_version = fields.Integer()
    occurred_on = fields.Date()


class EventMeta(ABCMeta):
    def __init__(cls, name, bases, namespace):
        if not hasattr(cls, 'events'):
            # This branch only executes when processing the mount point itself.
            # So, since this is a new plugin type, not an implementation, this
            # class shouldn't be registered as a plugin. Instead, it sets up a
            # list where plugins can be registered later.
            cls.events = []
        else:
            # This must be a plugin implementation, which should be registered.
            # Simply appending it to the list is all that's needed to keep
            # track of it later.
            cls.events.append(cls)

class Event(metaclass=EventMeta):

    # A constant that subscriber can use in their "listens_to" events to
    # tell they are interested in all the events that happen on their topic
    ALL = 'ALL'

    @property
    @abstractmethod
    def event_type_name(self):
        raise NotImplemented()

    def __init__(self, event_version=1, occurred_on=datetime.now()):
        self.occurred_on = occurred_on
        self.event_version = event_version

    def get_event_version(self):
        return self.event_version

    def get_occurred_on(self):
        return self.occurred_on

    def get_event_type_name(self):
        return self.event_type_name
