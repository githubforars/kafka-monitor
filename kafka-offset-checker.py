#!/usr/local/bin/python2.7
from pykafka import KafkaClient
import pykafka.cli.kafka_tools  as kafka_tools
import argparse
import calendar
import datetime as dt
import sys
import time
import pykafka
from pykafka.common import OffsetType
from pykafka.protocol import PartitionOffsetCommitRequest
from pykafka.utils.compat import PY3, iteritems
import argparse
import socket
import time
import re

graphite_server = 'x.x.x.x'
graphite_port= '2003'
hostname = socket.gethostname()


def stringsub(val):
    return(re.sub(r'\.', '_',val))

def fetch_offsets(client, topic, offset):
    if offset.lower() == 'earliest':
        return topic.earliest_available_offsets()
    elif offset.lower() == 'latest':
        return topic.latest_available_offsets()
    else:
        offset = dt.datetime.strptime(offset, "%Y-%m-%dT%H:%M:%S")
        offset = int(calendar.timegm(offset.utctimetuple())*1000)
        return topic.fetch_offset_limits(offset)


def fetch_consumer_lag(client, topic, consumer_group):
    try:
      latest_offsets = fetch_offsets(client, topic, 'latest')
      consumer = topic.get_simple_consumer(consumer_group=consumer_group,
                                         auto_start=False)
      current_offsets = consumer.fetch_offsets()
      return {p_id: (latest_offsets[p_id].offset[0], res.offset)
            for p_id, res in current_offsets}
    except AttributeError:
      pass


def get_consumer_lag(client, topic, consumer_group):
    try:
      lag_info = fetch_consumer_lag(client, topic, consumer_group)
      for k,v in iteritems(lag_info):
         print('server.'+stringsub(hostname)+'.kafka.'+stringsub(topic.name)+'.'+str(k),v[0]-v[1])
      #   graphite_update('servers.'+stringsub(hostname)+'.kafka.'+stringsub(topic.name)+'.'+str(k),v[0]-v[1])
    except AttributeError:
      pass


def get_consumer_groups_offsets(client):
      consumer_groups = {}
      brokers = client.brokers
      obj_topics = client.topics
      disconnected_topic = []
      excluded_group = ['__consumer_offsets']
      for broker_id, broker in brokers.iteritems():
        groups = broker.list_groups().groups.keys()
        groups_metadata = broker.describe_groups(group_ids=groups).groups
      for group_id, describe_group_response in groups_metadata.iteritems():
         members = describe_group_response.members
         for member_id, member in members.iteritems():
                topics = member.member_metadata.topic_names
                try:
                  if group_id in consumer_groups[topics[0]]:
                    pass
                  else:
                    consumer_groups[topics[0]].append(group_id)
                except KeyError:
                  consumer_groups[topics[0]] = [group_id]
      for topic in client.topics:
                if topic not in consumer_groups:
                    if topic not in excluded_group:
                         disconnected_topic.append(topic)
                         pass
                    else:
                         pass
                    pass
                else:
                    topics = obj_topics[topic]
                    #print(topic,consumer_groups[topic])
                    get_consumer_lag(client, topics, consumer_groups[topic][0])
      if disconnected_topic:
         print('Topics does not have consumer attached;\n{}'.format(disconnected_topic))
         sys.exit(2)
      else:
         print("All topics are having consumr attached")
         sys.exit(0)

def graphite_update(metric_path, value):
    timestamp = int(time.time())
    message = '%s %s %d\n' % (metric_path, value, timestamp)
    sock = socket.socket()
    sock.connect((graphite_server, int(graphite_port)))
    sock.sendall(message)
    sock.close()

if __name__ == '__main__':
      client = KafkaClient(hosts='kafka_server:9092',zookeeper_hosts='zookeeper:2181')
      get_consumer_groups_offsets(client)
