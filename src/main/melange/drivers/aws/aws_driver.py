import json
import uuid

import boto3

from melange.messaging import MessagingDriver, Message


class AWSDriver(MessagingDriver):
    def __init__(self):
        super().__init__()

    def declare_topic(self, topic_name):
        sns = boto3.resource('sns')
        topic = sns.create_topic(Name=topic_name)
        return topic

    def get_queue(self, queue_name):
        sqs_res = boto3.resource('sqs')

        return sqs_res.get_queue_by_name(QueueName=queue_name)

    def declare_queue(self, queue_name, *topics_to_bind, dead_letter_queue_name=None):
        sqs_res = boto3.resource('sqs')

        queue = sqs_res.create_queue(QueueName=queue_name)

        if topics_to_bind:
            statements = []
            for topic in topics_to_bind:
                statement = {
                    'Sid': 'Sid{}'.format(uuid.uuid4()),
                    'Effect': 'Allow',
                    'Principal': '*',
                    'Resource': queue.attributes['QueueArn'],
                    'Action': 'sqs:SendMessage',
                    'Condition': {
                        'ArnEquals': {
                            'aws:SourceArn': topic.arn
                        }
                    }
                }

                statements.append(statement)
                topic.subscribe(Protocol='sqs', Endpoint=queue.attributes['QueueArn'])

            policy = {
                'Version': '2012-10-17',
                'Id': 'sqspolicy',
                'Statement': statements
            }

            queue.set_attributes(Attributes={'Policy': json.dumps(policy)})

        dead_letter_queue = None
        if dead_letter_queue_name:
            dead_letter_queue = sqs_res.create_queue(QueueName=dead_letter_queue_name)

            redrive_policy = {
                'deadLetterTargetArn': dead_letter_queue.attributes['QueueArn'],
                'maxReceiveCount': '4'
            }

            queue.set_attributes(Attributes={'RedrivePolicy': json.dumps(redrive_policy)})

        return queue, dead_letter_queue

    def retrieve_messages(self, queue):
        messages = queue.receive_messages(MaxNumberOfMessages=1, VisibilityTimeout=100,
                                          WaitTimeSeconds=10, AttributeNames=['All'])

        return [Message(message.message_id, self._extract_message_content(message), message)
                for message in messages]

    def publish(self, content, topic):
        response = topic.publish(Message=content)

        if 'MessageId' not in response:
            raise ConnectionError('Could not send the event to the SNS TOPIC')

    def acknowledge(self, message):
        message.metadata.delete()

    def close_connection(self):
        pass

    def delete_queue(self, queue):
        queue.delete()

    def delete_topic(self, topic):
        topic.delete()

    def _extract_message_content(self, message):
        body = message.body
        message_content = json.loads(body)
        if 'Message' in message_content:
            content = json.loads(message_content['Message'])
        else:
            content = message_content

        return content
