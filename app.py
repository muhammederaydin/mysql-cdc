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

import logging
import time, os
import ast, csv
from Base import Base

logging.basicConfig(filename="app.log")


class CDC(Base):

    def __init__(self):
        Base.__init__(self)
        self.filename = self.log_dir
        self.consumer = self.get_consumer()
    
    def consumed_query(self):
        try:
            for message in self.consumer:
                self.query_finder(message)
        except Exception as e:
            logging.error('Execute Query Finder Error: {}'.format(str(e)))
            raise('Execute Query Finder Error: {}'.format(str(e)))

    def query_finder(self, query):
        """
        Parse MySQL query and call functions for insert and
        update operations

        query: MySQL query which reading from general.log file
        """
        try:
            splitted_query = query.split(" ")
            method = splitted_query[0].lower()
            if "insert" in method:
                self.publish_insert_query(query)
                self.check_insert_done(query)
            elif "update" in method:
                self.publish_update_query(query)
                self.check_update_done(query)
            elif "delete" in method:
                self.publish_delete_query(query)
            else:
                print("Unsupported Method")
        except Exception as e:
            logging.error("Query Method Finder Error: {}".format(str(e)))
            raise("Query Method Finder Error: {}".format(str(e)))

    def publish_insert_query(self, query):
        try:
            self.publish_message(key = "insert", value = query, 
                                 topic = "cdc_insert")
        except Exception as e:
            logging.error('Publish Insert Query Error: {}'.format(str(e)))
            raise('Publish Insert Query Error: {}'.format(str(e)))

    def publish_update_query(self, query):
        try:
            self.publish_message(key = "update", value = query, 
                                 topic = "cdc_update")
        except Exception as e:
            logging.error('Publish Update Query Error: {}'.format(str(e)))
            raise('Publish Update Query Error: {}'.format(str(e)))

    def publish_delete_query(self, query):
        try:
            self.publish_message(key = "delete", value = query,
                                 topic = "cdc_delete")
        except Exception as e:
            logging.error('Publish Delete Query Error: {}'.format(str(e)))
            raise('Publish Delete Query Error: {}'.format(str(e)))
