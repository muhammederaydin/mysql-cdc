#MIT License
#
#Copyright (c) 2020 Muhammed Eraydın
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.

import threading
from kafka import KafkaConsumer, KafkaProducer
import json
from Config import config
import logging
from sys import exit
import sys
logger = logging.getLogger('kafka')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


class Base(object):

    def __init__(self):
        super(Base, self).__init__()
        self.debug = True
        self.config = config
        self._consumer = None
        self._producer = None
        self.log_dir = config["general_log_dir"]
        self.topic = config['topic']
        self.key = "MySQL"
        self.listener_thread = None

    def get_consumer(self):
        try:
            self._consumer = KafkaConsumer(
                self.config["topic"],  # topic name
                bootstrap_servers=self.config["bootstrap_servers"],
                auto_offset_reset='earliest',
                group_id=self.config["consumer_group_id"],
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
            logging.info("Kafka Consumer is UP!")
            return self._consumer
        except Exception as e:
            logging.error("Kafka Consumer can not be started! \
                              The exception was: %s" % str(e))
            raise("Kafka Consumer can not be started! \
                              The exception was: %s" % str(e))

    def get_producer(self):
        try:
            self._producer = KafkaProducer(
                client_id=self.config["producer_group_id"],
                bootstrap_servers=self.config["bootstrap_servers"],
            )
            logging.info("Kafka Producer is UP!")
        except Exception as e:
            logging.error("Kafka Producer can not be started! \
                              The exception was: %s" % str(e))
            raise("Kafka Producer can not be started! \
                              The exception was: %s" % str(e))

    def publish_message(self, value, key, topic):
        try:
            key_bytes = bytes(key, encoding='utf-8')
            value_bytes = bytes(value, encoding='utf-8')
            self._producer.send(topic, key=key_bytes, value=value_bytes)
            self._producer.flush()
            if self.debug:
                logging.info("Message published successfully. \
                                 Key:%s, Value:%s" % (key, value))
        except Exception as e:
            if self.debug:
                raise("Exception in publishing message. \
                             Key:%s, Value:%s" % (key, value))
            logging.info("Exception in publishing message. \
                             Key:%s, Value:%s" % (key, value))

