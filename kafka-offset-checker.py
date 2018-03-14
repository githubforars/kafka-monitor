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
      return {p_id: (latest_offsets[p_id].offset[0], res.offset if res.offset != -1 else latest_offsets[p_id].offset[0])
            for p_id, res in current_offsets}
    except AttributeError:
      pass


def get_consumer_lag(client, topic, consumer_group):
    try:
      lag_info = fetch_consumer_lag(client, topic, consumer_group)
      for k,v in iteritems(lag_info):
        graphite_update('servers.'+stringsub(hostname)+'.kafka.'+stringsub(topic.name)+'.'+str(k),v[0]-v[1])
        print('server.'+stringsub(hostname)+'.kafka.'+stringsub(topic.name)+'.'+str(k),v[0]-v[1],consumer_group)
    except AttributeError:
      pass

def get_consumer_groups_offsets(client):
      consumer_groups = {}
      brokers = client.brokers
      obj_topics = client.topics
      disconnected_topic = []
      groups=[]
      topics=[]
      active_topics=[]
      excluded_group = ['__consumer_offsets','KafkaManagerOffsetCache']
      for broker_id, broker in brokers.iteritems():
        groups= broker.list_groups().groups.keys()
        groups_metadata = broker.describe_groups(group_ids=groups).groups
        for group_id, describe_group_response in groups_metadata.iteritems():
          if group_id not in excluded_group:
            members = describe_group_response.members
          for member_id, member in members.iteritems():
                topics = member.member_metadata.topic_names
                for topic in topics:
                  if topic in active_topics:
                     pass
                  else:
                    active_topics.extend(topics)
                    get_consumer_lag(client, obj_topics[topic], group_id)
      for topic in client.topics:
             if topic not in active_topics:
                 disconnected_topic.append(topic)
             else:
                 pass
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
