import os
import argparse
from google.cloud import pubsub
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ='key.json'

def run(args):
    project_id = args.project
    subscriber = pubsub.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, 'dataflow-pubsub')

    response = subscriber.pull(
        request={
            'subscription': subscription_path,
            'max_messages': 1,
        }
    )

    for message in response.received_messages:
        filename=message.data
        filename = filename['name']
        print(filename)

    subscriber.acknowledge(
        request={
            'subscription': subscription_path,
            'ack_ids': [message.ack_id],
        }
    )

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument("--project", required=True)
  args = parser.parse_args()
  run(args)