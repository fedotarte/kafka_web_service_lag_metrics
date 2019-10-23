import logging
import config
from flask import Flask, Response, abort
from model_metrics.kafka_metrics_exporter import MetricsExporter

brokers_host = config.kafka['brokerhost']
brokers_port = config.kafka['port']
is_sasl = config.kafka['is_sasl']
security_protocol = config.kafka['security_protocol']
sasl_mechanism = config.kafka['sasl_mechanism']
c_sasl_username = config.kafka['sasl_plain_username']
c_sasl_password = config.kafka['sasl_plain_password']


def create_logger(level=logging.INFO):
    log = logging.getLogger("kafka_exporter")
    log.setLevel(level)
    # handler = logging.StreamHandler(sys.stdout)
    fh = logging.FileHandler("trace.log")
    fh.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - PID:%(process)d - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    log.addHandler(fh)
    return log


log = create_logger(level=logging.INFO)
app = Flask(__name__)


@app.route('/')
def main_url():
    topic_name = "topic_main"
    partition = "0"
    size = 1234567890121212
    string_topic = 'kafka_server_topic_size {topicname=\"%s\", partition = \"%s\",}: %.1f' % (topic_name, partition, size)
    response = Response(string_topic, status=200, content_type='text/plain; charset=utf-8')
    return response


@app.route('/metrics', methods=['GET'])
def return_metrics():
    metrics_response = []
    # if is_sasl is true - add sasl_user_name adn sasl_password
    if not is_sasl:
        me = MetricsExporter(brokers_host, brokers_port)
    else:
        print("Metric Exporter with sasl activated")
        me = MetricsExporter(brokers_host, brokers_port, is_sasl, sasl_username=c_sasl_username, sasl_password=c_sasl_password)
    me.init_connection()
    consumer_group_list = me.get_groups()
    log.info('consumer_group_list: %s' % consumer_group_list)
    for consumer in consumer_group_list:
        me.get_topic_offsets(consumer)
    log.info('kafka_groups_response %s' % str(me.consumer_groups))
    log.info('kafka_topics_response %s' % str(me.topic_offsets_for_groups))
    metrics_response = me.topic_offsets_for_groups
    log.info("topic_list: %s" % str(set(me.topic_list)))
    log.info("topic data for metrics: %s" % str(me.topic_data_metric))
    # get_metrics = "get metrics here"
    if len(metrics_response) == 0:
        abort(404)
    me.close_connection()
    response = Response(metrics_response, status=200, content_type='text/plain; charset=utf-8')
    return response


# @app.route('/entity/<int:entity_id>')
# def entity_detail(entity_id):
#     entity = search_entity(entity_list, entity_id)
#     if book is None:
#         abort(404)
#
#     content = json.dumps(book)
#     return content, 200, {'Content-Type': JSON_MIME_TYPE}


@app.errorhandler(404)
def not_found(e):
    return '', 404
