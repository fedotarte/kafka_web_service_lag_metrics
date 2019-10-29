import socket
from kafka import BrokerConnection
from kafka import TopicPartition
from kafka import SimpleClient
from kafka.consumer import KafkaConsumer
from kafka.protocol.admin import *
from kafka.protocol.commit import OffsetFetchRequest_v3


# from confluent_kafka import Consumer
# from confluent_kafka import TopicPartition


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
        self.sasl_plain_username = ""
        self.sasl_plain_password = ""
        if sasl_username is not None and sasl_password is not None:
            self.sasl_plain_username = sasl_username,
            self.sasl_plain_password = sasl_password
        self.brokers_host = host
        self.brokers_port = port
        self.is_sasl = is_sasl
        self.kafka_groups_response = None
        self.kafka_topics_response = None
        self.consumer_groups = []
        self.topic_offsets_for_groups = []
        self.topic_offsets_for_rate = []
        self.topic_nps = {}
        self.topic_data_metric = []
        self.topic_list = []
        self.consumer_offset_metric = []
        self.bc = None
        if not self.is_sasl:
            self.bc = BrokerConnection(self.brokers_host,
                                       self.brokers_port,
                                       socket.AF_INET)
            print(self.bc.__str__())
            print("connection successful : %s" % str(self.bc.connected()))

        else:
            self.bc = BrokerConnection(self.brokers_host,
                                       self.brokers_port,
                                       socket.AF_INET,
                                       security_protocol='SASL_PLAINTEXT',
                                       sasl_mechanism='PLAIN',
                                       sasl_plain_username='admin',
                                       sasl_plain_password='12345')
            print(self.bc.__str__())
            print("connection with sasl successful : %s" % str(self.bc.connected()))

    def get_groups(self):
        cons_groups = []
        list_groups_request = ListGroupsRequest_v1()
        self.kafka_groups_response = self.bc.send(list_groups_request)
        while not self.kafka_groups_response.is_done:
            for resp, f in self.bc.recv():
                print('group resp:', resp)
                f.success(resp)
        for group in self.kafka_groups_response.value.groups:
            cons_groups.append(group[0])
            # print('group[0]', group[0])
        return cons_groups

    # invoke in each iteration of group list

    def get_topic_offsets(self, consumer_group):
        fetch_offset_request = OffsetFetchRequest_v3(consumer_group, None)
        print("offset fetch request: ", fetch_offset_request)
        self.kafka_topics_response = self.bc.send(fetch_offset_request)
        while not self.kafka_topics_response.is_done:
            for resp, f in self.bc.recv():
                # print('resp: ', resp)
                f.success(resp)
        for topic in self.kafka_topics_response.value.topics:
            self.topic_list.append(topic[0])
            print("topic: ", topic)
            print("topic[0]", topic[0])
            print("topic[1]", topic[1])
            str_broker_host = self.brokers_host + ':' + str(self.brokers_port)
            con = self.invoke_kafka_consumer(str_broker_host, self.is_sasl)
            print("connection : %s" % con.bootstrap_connected())
            print("consumer config is: ", con.config)
            # TODO uncomment if necessary
            # ps = [TopicPartition(topic[0], p) for p in con.partitions_for_topic(topic[0])]
            # print("ps: ", ps)
            end_offset = con.end_offsets([TopicPartition(topic[0], 0)])
            # print('end_offset: ', end_offset)
            topic_size = [*end_offset.values()]
            # here to make list
            # print('topic size: ', topic_size[0])
            #
            # print('offsets for {0}'.format(topic[0]))
            # topic[1] is like  [(0, 76, '', 0)]
            for partition in topic[1]:
                print("partition in topic cycle: ", partition)
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
                # add new item to metrics list
                self.topic_offsets_for_groups.append(topic_consumer_lag_model.__str__())
                # extending list of consumer metrics by other list with rate metrics to combine the response
                self.topic_offsets_for_groups.extend(topic_consumer_rate_model.__str__())
                con.close()
        return self.topic_offsets_for_groups

    def get_topic_inbound_rate(self, consumer_group, topic_name):
        pass

    def get_topic_outbound_rate(self):
        pass

    def close_connection(self):
        self.bc.close()

    def init_connection(self):
        if self.bc is not None:
            self.bc.connect_blocking()
            print(self.bc.__str__())
            print("connection __SASL__ successful : %s" % str(self.bc.connected()))

    def invoke_kafka_consumer(self, p_str_broker_host, p_is_sasl):
        if p_is_sasl:
            # consumer = Consumer({
            #     'bootstrap.servers': config.BOOTSTRAP_SERVERS,
            #     'group.id': config.CONSUMER_GROUP,
            #     'enable.auto.commit': False,
            # })

            return KafkaConsumer(bootstrap_servers=p_str_broker_host,
                                 security_protocol='SASL_PLAINTEXT',
                                 sasl_mechanism='PLAIN',
                                 sasl_plain_username='admin',
                                 sasl_plain_password='12345')
        else:
            return KafkaConsumer(bootstrap_servers=p_str_broker_host)


