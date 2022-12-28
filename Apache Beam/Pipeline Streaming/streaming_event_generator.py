# Import the necessary modules
import time
import os
import json
from google.cloud import pubsub_v1

#Credenciales
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="../service-account.json"

# Create a Publisher client
publisher = pubsub_v1.PublisherClient()

# Set the topic name and project ID
topic_name = 'projects/dt-data-analytics/topics/MyTopicTest'

def get_next_event():
    return {"message": "hola", "number": 4}

def streaming_event_generator():
  # Generate and publish events in a loop
  event_generator = get_next_event()
  while True:
    event = event_generator  # Get the next event
    data = json.dumps(event).encode('utf-8')  # Convert the event to a byte string
    publisher.publish(topic_name, data=data)
    print('Event published: {}'.format(event))
    time.sleep(1)  # Wait for a second before generating the next event


if __name__ == "__main__":
    # Run the generator in a loop
    streaming_event_generator()
