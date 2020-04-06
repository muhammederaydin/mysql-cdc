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

from Base import Base
from os import stat
from time import sleep
import logging


class LogReader(Base):
    
    def __init__(self):
        Base.__init__(self)

    def read_file(self):
        """
        Read MySQL general.log file and if query executed catch the
        query. After that call query_finder function
        """
        try:
            #Open the file
            file = open(self.log_dir,'r')

            #Find the size of the file and move to the end
            st_results = stat(self.log_dir)
            st_size = st_results[6]
            file.seek(st_size)
            while 1:
                where = file.tell()
                line = file.readline()
                if not line:
                    sleep(1)
                    file.seek(where)
                else:
                    if "Query" in line:
                        _q = line.split("Query")[1]
                        _q = _q.lower()
                        _q = _q.replace("\t","")
                        self.publish_message(key = "MySQL", value = _q, 
                                         topic = self.topic)
        except Exception as e:
            logging.error('Log File Reader Error: {}'.format(str(e)))
            raise('Log File Reader Error: {}'.format(str(e)))

