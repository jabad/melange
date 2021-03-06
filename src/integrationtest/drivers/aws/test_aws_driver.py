import json
import uuid

from melange.drivers.aws import AWSDriver


class TestAWSDriver:

    def setup_method(self):
        self.topic = None
        self.topic_1 = None
        self.topic_2 = None
        self.queue = None
        self.dead_letter_queue = None

        self.driver = AWSDriver()

    def teardown_method(self):
        if self.topic:
            self.topic.delete()

        if self.topic_1:
            self.topic_1.delete()

        if self.topic_2:
            self.topic_2.delete()

        if self.queue:
            self.queue.delete()

        if self.dead_letter_queue:
            self.dead_letter_queue.delete()

    def test_create_topic(self):
        self.topic = self.driver.declare_topic(self._get_topic_name())
        assert self.topic.arn

    def test_create_normal_queue(self):
        self.queue, _ = self.driver.declare_queue(self._get_queue_name())
        assert self.queue.url
        assert 'Policy' not in self.queue.attributes

    def test_create_queue_and_bind_to_topic(self):
        self.topic = self.driver.declare_topic(self._get_topic_name())
        self.queue, _ = self.driver.declare_queue(self._get_queue_name(), self.topic)

        assert self.queue.url

        policy = json.loads(self.queue.attributes['Policy'])
        policy_topic_arn = policy['Statement'][0]['Condition']['ArnEquals']['aws:SourceArn']
        assert policy_topic_arn == self.topic.arn

    def test_create_queue_and_bind_several_topics(self):
        self.topic_1 = self.driver.declare_topic(self._get_topic_name())
        self.topic_2 = self.driver.declare_topic(self._get_topic_name())
        self.queue, _ = self.driver.declare_queue(self._get_queue_name(), self.topic_1, self.topic_2)

        assert self.queue.url

        policy = json.loads(self.queue.attributes['Policy'])
        policy_topic_arn = policy['Statement'][0]['Condition']['ArnEquals']['aws:SourceArn']
        assert policy_topic_arn == self.topic_1.arn

        policy_topic_arn = policy['Statement'][1]['Condition']['ArnEquals']['aws:SourceArn']
        assert policy_topic_arn == self.topic_2.arn

    def test_create_queue_with_dead_letter_queue(self):
        self.topic = self.driver.declare_topic(self._get_topic_name())
        self.queue, self.dead_letter_queue = self.driver.declare_queue(self._get_queue_name(),
                                                         self.topic,
                                                         dead_letter_queue_name=self._get_queue_name())

        assert self.queue.url

        attrs = self.queue.attributes

        policy = json.loads(attrs['Policy'])
        policy_topic_arn = policy['Statement'][0]['Condition']['ArnEquals']['aws:SourceArn']
        assert policy_topic_arn == self.topic.arn

        redrive_policy = json.loads(attrs['RedrivePolicy'])
        queue_arn = self.dead_letter_queue.attributes['QueueArn']

        assert redrive_policy['deadLetterTargetArn'] == queue_arn
        assert redrive_policy['maxReceiveCount'] == 4

    def _get_queue_name(self):
        return 'test_queue_{}'.format(uuid.uuid4())

    def _get_topic_name(self):
        return 'test_queue_{}'.format(uuid.uuid4())
