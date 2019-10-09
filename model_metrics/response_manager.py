from model_metrics.kafka_metrics_exporter import MetricsExporter



class ResponseManager:
    def __init__(self):
        self.topic_metric_pattern = {}
        # self.me = MetricsExporter(brokers_host, brokers_port)
        pass

    def topic_metric(self):
        metrics_response = []
        string_topic = ""
        consumer_group_list = self.me.get_groups()
        print('consumer_group_list: ', consumer_group_list)
        for consumer in consumer_group_list:
            self.me.get_topic_offsets(consumer)
        # print('kafka_groups_response', me.consumer_groups)
        print('kafka_topics_response', self.me.topic_offsets_for_groups)
        list_of_topics = self.me.topic_list
        deduplicated_list = set(list_of_topics)
        for topic_name in list(deduplicated_list):
            pass
            # topic_metric_item = TopicMetricModel(topic_name, )
            # "%s, \"%s\" is not an age!" % (name, age)

            # metrics_response = 'kafka_server_topic_size {topicname=\"%s\", partition = \"%s\" } %s' % (topic_name, partition, size())
        # get_metrics = "get metrics here"
        string_topic = 'kafka_server_topic_size {topicname=\"%s\", partition = \"%s\",}: %.1f'
        # % (self.topic_metric_pattern,
        #    self.topic_metric_pattern['partition'],
        #    self.topic_metric_pattern['size'])
        return string_topic

    def consumer_offset_metric(self):
        pass
