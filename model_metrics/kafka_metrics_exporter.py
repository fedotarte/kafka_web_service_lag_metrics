import socket
from kafka import BrokerConnection
from kafka import TopicPartition
from kafka.consumer import KafkaConsumer
from kafka.protocol.admin import *
from kafka.protocol.commit import OffsetFetchRequest_v3


class TopicNamePartitionSizeModel:
    def __init__(self, topic_name, topic_partition, topic_size):
        self._topic_name = topic_name
        self._topic_partition = topic_partition
        self._topic_size = int(topic_size)

    # def set_topic_name(self, topic_name):
    #     self.__topic_name = topic_name

    def get_topic_name(self):
        return self._topic_name

    def get_topic_partition(self):
        return self._topic_partition

    def get_topic_size(self):
        return self._topic_size

    def __str__(self):
        # 'kafka_server_topic_size {topicname=\"%s\", partition = \"%s\",}: %.1f' % (topic_name, partition, size)
        return 'kafka_topic_size {topicname=\"%s\", partition = \"%s\",}: %.1f' % (self._topic_name, self._topic_partition, self._topic_size)


class SimpleTopicConsumerLagMetricModel:
    def __init__(self, consumer_group, topic_name, topic_partition, topic_size, topic_offset):
        self._consumer_group = consumer_group
        self._topic_name = topic_name
        self._topic_partition = topic_partition
        self._topic_size = topic_size
        self._topic_offset = topic_offset
        self.diff = int(self._topic_size) - int(self._topic_offset)
        # self._topic_lag = topic_lag

    # def set_topic_name(self, topic_name):
    #     self.__topic_name = topic_name
    def get_consumer_group(self):
        return self._consumer_group

    def get_topic_name(self):
        return self._topic_name

    def get_topic_partition(self):
        return self._topic_partition

    def get_topic_size(self):
        return self._topic_size

    def get_topic_offset(self):
        return self._topic_offset

    # def get_topic_lag(self):
    #     self.diff = int(self._topic_size) - int(self._topic_offset)

    def __str__(self):
        return 'kafka_topic_consumer_lag{consumer_group=\"%s\",' \
               'topic_name=\"%s\",' \
               'partition=\"%s\",' \
               'size=\"%s\",' \
               'offset=\"%s\",} %.1f \n' \
               % (self._consumer_group,
                  self._topic_name,
                  self._topic_partition,
                  self._topic_size,
                  self._topic_offset,
                  self.diff)


class SimpleRateLagMetricModel(SimpleTopicConsumerLagMetricModel):
    def __init__(self, consumer_group, topic_name, topic_partition, topic_size, topic_offset):
        super().__init__(consumer_group, topic_name, topic_partition, topic_size, topic_offset)
        self._consumer_group = consumer_group
        self._topic_name = topic_name
        self._topic_partition = topic_partition
        self._topic_size = topic_size
        self._topic_offset = topic_offset
        self.diff = int(self._topic_size) - int(self._topic_offset)

    def __str__(self):
        return 'kafka_topic_consumer_lag_for_rate{' \
               'consumer_group=\"%s\",' \
               'topic_name=\"%s\",' \
               'partition=\"%s\",} %.1f \n' \
               % (self._consumer_group,
                  self._topic_name,
                  self._topic_partition,
                  self.diff)


class MetricsExporter:
    def __init__(self, host, port, is_sasl=False, sasl_username=None, sasl_password=None):
        if sasl_username is not None and sasl_password is not None:
            self.sasl_plain_username = sasl_username,
            self.sasl_plain_password = sasl_password
        self.brokers_host = host
        self.brokers_port = port
        self.kafka_groups_response = None
        self.kafka_topics_response = None
        self.consumer_groups = []
        self.topic_offsets_for_groups = []
        self.topic_offsets_for_rate = []
        self.topic_nps = {}
        self.topic_data_metric = []
        self.topic_list = []
        self.consumer_offset_metric = []
        if not is_sasl:
            self.bc = BrokerConnection(self.brokers_host,
                                       self.brokers_port,
                                       socket.AF_INET)

        else:
            self.bc = BrokerConnection(self.brokers_host,
                                       self.brokers_port,
                                       socket.AF_INET,
                                       security_protocol='SASL_PLAINTEXT',
                                       sasl_mechanism='PLAIN',
                                       sasl_plain_username=self.sasl_plain_username,
                                       sasl_plain_password=self.sasl_plain_password)

        # self.bc.connect_blocking()

    def get_groups(self):
        cons_groups = []
        list_groups_request = ListGroupsRequest_v1()
        self.kafka_groups_response = self.bc.send(list_groups_request)
        while not self.kafka_groups_response.is_done:
            for resp, f in self.bc.recv():
                # print('group resp:', resp)
                f.success(resp)
        for group in self.kafka_groups_response.value.groups:
            cons_groups.append(group[0])
            # print('group[0]', group[0])
        return cons_groups

    # invoke in each iteration of group list

    def get_topic_offsets(self, consumer_group):
        fetch_offset_request = OffsetFetchRequest_v3(consumer_group, None)
        self.kafka_topics_response = self.bc.send(fetch_offset_request)
        print('kafka_topics_response', self.kafka_topics_response)
        while not self.kafka_topics_response.is_done:
            for resp, f in self.bc.recv():
                # print('resp: ', resp)
                f.success(resp)
        for topic in self.kafka_topics_response.value.topics:
            self.topic_list.append(topic[0])
            str_broker_host = self.brokers_host + ':' + str(self.brokers_port)
            con = KafkaConsumer(bootstrap_servers=str_broker_host)
            ps = [TopicPartition(topic[0], p) for p in con.partitions_for_topic(topic[0])]
            # print("ps: ", ps)
            end_offset = con.end_offsets(ps)
            # print('end_offset: ', end_offset)
            topic_size = [*end_offset.values()]
            # here to make list
            # print('topic size: ', topic_size[0])
            #
            # print('offsets for {0}'.format(topic[0]))

            # TODO kafka_topic_size{topic=="TestTopic", partition="0"} 0.0

            for partition in topic[1]:
                # print("topic[1]", topic[1])
                # print('- partition {0}, offset: {1} lag: {2}'.format(partition[0], int(partition[1]), lag_diff))

                # data for lag and offset metrics
                topic_consumer_lag_model = SimpleTopicConsumerLagMetricModel(str(consumer_group),
                                                                             str(topic[0]),
                                                                             str(partition[0]),
                                                                             str(topic_size[0]),
                                                                             str(partition[1]))
                # data for rate and top 5 metrics
                topic_consumer_rate_model = SimpleRateLagMetricModel(str(consumer_group),
                                                                     str(topic[0]),
                                                                     str(partition[0]),
                                                                     str(topic_size[0]),
                                                                     str(partition[1]))

                self.topic_offsets_for_groups.append(topic_consumer_lag_model.__str__())
                self.topic_offsets_for_groups.extend(topic_consumer_rate_model.__str__())
        return self.topic_offsets_for_groups

    def get_topic_inbound_rate(self, consumer_group, topic_name):
        topic_consumer_lag_rate_model = SimpleTopicConsumerLagMetricModel
        self.topic_offsets_for_rate.append(consumer_group, topic_name)
        pass

    def get_topic_outbound_rate(self):
        pass

    def close_connection(self):
        self.bc.close()

    def init_connection(self):
        self.bc.connect_blocking()
