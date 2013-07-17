# Copyright (c) 2010-2012 OpenStack, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import os
import sys
import md5
from re import sub
import threading
import traceback
import sqlite3
import random
from struct import unpack_from

from swift.common import utils
from swift.common import configuration
from swift.common.configuration import account_dirs, container_dirs, \
    obj_dirs
from swift.common.ring import Ring
#from swift.common.db import AccountBroker, ContainerBroker
"""
This module offers a plugable generic swift crawler for datadirs
It uses a singel crawl to activate plugable services such as auditors,
updaters, replicators, synchronization and federation.

The single crawl reduces the resoucre use as comapred to the existing
multi-daemon scheme. 

A single process is used to activate a single crawl across the server data.
A thread is used for each datadir of each device. 
The admin should define a target cycle for eventual consistency and tune
the system accordingly. For exampel an admin may choose to two 10 hour cycles
per day, the first starting at 10PM and the second starting at 8AM such that
no eventual consistency activity is done during his service peak hours
(6PM-10PM in this example). 

In this example, the admin would want to tune all eventual consistency
to be done evenly along the 10 hour cycles. This can be achieved by tuiing
the crawler threads to spread their resource load across the target 10hour
cycles.

For example if a crawler thread has 100000 parts, taking 1 hour in full speed,
it may be required to set the thread slowdown to be 9/10*3600 seconds such
that the thread will introduce less load on the system and complete its task
within 10 hours and not within 1. 

The crawler collects teh necessery information to tune the system into an
sqlite3 DB allowing tunning to be achieved every now and than. 

"""



def listdir(path):
    try:
        res = os.listdir(path)
        if res == None:
            res = []
    except:
        res = []
    return res


class Crawler(object):
    def __init__(self, conf_path):
        self.conf_path = conf_path
        # Ensure TZ environment variable exists to avoid stat('/etc/localtime')
        # on some platforms. Reported times is locked to the timezone in which
        # the server first starts running in locations that periodically change
        # timezones.
        os.environ['TZ'] = time.strftime("%z", time.gmtime())
        self.threads = []
        self.dirs = {'object': obj_dirs,
                     'container': container_dirs,
                     'account': account_dirs}

    def read_conf(self):
        self.conf = utils.readconf(self.conf_path)
        conf = self.conf['crawler']
        self.devices = conf.get('devices', '/srv/node')
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.rings = {'object': Ring(self.swift_dir, ring_name='object'),
                      'container': Ring(self.swift_dir, ring_name='container'),
                      'account': Ring(self.swift_dir, ring_name='object')}
        self.log_name = conf.get('log_name', 'Crawler')
        self.logger = utils.get_logger(conf, self.log_name)
        self.mount_check = \
            utils.config_true_value(conf.get('mount_check', True))
        self.round_pause = int(conf.get('round_pause', 60))
        # disable fallocate if desired
        if utils.config_true_value(conf.get('disable_fallocate', 'no')):
            utils.disable_fallocate()
        # set utils.FALLOCATE_RESERVE if desired
        reserve = int(conf.get('fallocate_reserve', 0))
        if reserve > 0:
            utils.FALLOCATE_RESERVE = reserve
        self.tasks = self.collect_tasks()

    def collect_tasks(self):
        tasks = {'account': {}, 'container': {}, 'object': {}}
        for t in tasks:
            for level in ('partition', 'suffix', 'hsh', 'item'): 
                tasks[t][level] = []
        for section in self.conf:
            try:
               if type(self.conf[section]) is not dict:
                   continue
               if 'data_type' in self.conf[section] and self.conf[section] :
                    dataname = self.conf[section]['data_type']
                    level = self.conf[section]['level']
                    task_module = self.conf[section]['task_module']
                    task_class = self.conf[section]['task_class']
                    task_conf = self.conf[section]['task_conf']
                    task_method = self.conf[section].get('task_method',
                        'process_%s' % level)
                    _module = __import__(task_module)
                    splits = task_module.split('.')
                    for split in splits[1:]:
                        _module = getattr(_module, split)
                    _class = getattr(_module, task_class)
                    conf = utils.readconf(task_conf, section)
                    _object = _class(conf)
                    self.conf[section]['func'] = getattr(_object, task_method)
                    self.conf[section]['cycles'] = \
                        int(self.conf[section].get('cycles', 1))
                    tasks[dataname][level].append(section)
            except: 
               print 'Failed to parse config file section %s - %s' % \
                   (section, sys.exc_info())
               traceback.print_exc()
        return tasks

    def crawl(self):
        print 'Restarting full cycle:'
        self.read_conf()
        self.db_initialize()
        self.db.increment_cycle()
        self.crawl_all()

    def set_crawl(self, device, datatype, slowdown):
        self.read_conf()
        self.db_initialize()
        dev_path = os.path.join(self.devices, device)
        self.db.set_runtime_params(dev_path, datatype, slowdown)
        
    def get_past_crawls(self):
        self.read_conf()
        self.db_initialize()
        return self.db.get_runtime_results()

    def db_initialize(self):
        self.db = CrawlerDb(self.conf)
        try: 
            self.db.connect()
        except:
            self.db.initialize()

    def crawl_all(self):
        print 'crawl_all'
        for device in listdir(self.devices):
            print device
            dev_path = os.path.join(self.devices, device)
            if self.mount_check and not os.path.ismount(dev_path):
                print 'failed mount check'
                self.logger.increment('errors')
                self.logger.warn(
                    _('Skipping %s as it is not mounted'), device)
                continue
            print 'calling crawl_device'
            self.crawl_device(dev_path)
        while threading.active_count() > 1:
            time.sleep(1)

    def crawl_device(self, dev_path):
        print 'crawl_device'
        for datatype in self.dirs:
            thread = DataCrawler(dev_path, datatype,
                                 self.tasks[datatype], self.dirs[datatype],
                                 self.conf)
            #thread.daemon = True
            thread.start()
            self.threads.append(thread)
   


           
class CrawlerDb(object):
    """
    Draft implementation for a DB used to gather DataCrawler runtime and stats
    results.
    The DB is also used to feed the necessery slowdown and cycle params 
    to the DataCrawler threads.
    It is envisioned that runtime results will be used to tune thread slowdown. 
    """
    def __init__(self, conf):
        self.conf = conf
        self.db_file = self.conf.get('db_file', '/var/swift/crawler.db')
        self.conn = None
        
    def connect(self):
        if self.conn is None:
            if not os.path.isfile(self.db_file):
                raise IOError('No such file')
            self.conn = sqlite3.connect(self.db_file, check_same_thread=False)

    def close(self):
        if self.conn is None:
            self.conn.close()
            self.conn = None

    def initialize(self):
        print 'initialize start'
        utils.mkdirs(os.path.dirname(self.db_file))
        self.conn = sqlite3.connect(self.db_file, check_same_thread=False) 
        self.conn.executescript("""
            CREATE TABLE data_crawlers (
                dev_path TEXT,
                datatype TEXT,
                slowdown FLOAT DEFAULT 0.001,
                cycle INTEGER DEFAULT 0,
                UNIQUE (dev_path, datatype)  
            );
            CREATE TABLE data_crawler_runtime (
                dev_path TEXT,
                datatype TEXT,
                cycle INTEGER,
                interval FLOAT,
                slowdown FLOAT, 
                parts INTEGER, 
                timestamp TEXT,
                UNIQUE (dev_path, datatype)  
            );
            """)                      
        self.conn.commit() 
        print 'initialize DONE'
 
    def increment_cycle(self):
        try:
            self.connect()
            c = self.conn.cursor()
            c.execute("UPDATE data_crawlers SET cycle = cycle + 1")
            self.conn.commit() 
        except sqlite3.Error as e:
            print 'increment_cycle exception %s' % e
        finally:
            c.close()

    def set_runtime_params(self, dev_path, datatype, slowdown):
        # Called to tune a system  - changing slowdown of a thread
        # Call self.get_runtime_params to insert the entry if missing...
        # TBD more efficient way to implement that INSERT/UPDATE
        self.get_runtime_params(dev_path, datatype)
        try:
            self.connect()
            c = self.conn.cursor()
            c.execute("UPDATE data_crawlers SET slowdown=? WHERE " 
                      "dev_path=? AND datatype=?",
                       (slowdown, dev_path, datatype))
            self.conn.commit() 
        except sqlite3.Error as e:
            print 'set_runtime_params exception %s' % e
        finally:
            c.close()

    def get_runtime_params(self, dev_path, datatype):
        # Called per cycle per thread
        try:
            self.connect()
            c = self.conn.cursor()
            c.execute("SELECT slowdown, cycle FROM data_crawlers WHERE "
                      "dev_path=? AND datatype=?", (dev_path, datatype))
            res = c.fetchone()
            if res == None:
                # Missing entry - add it!
                res = (0.001,  random.randint(0, 255))
                c.execute("INSERT INTO data_crawlers VALUES (?, ?, ?, ?)", \
                          (dev_path, datatype, res[0], res[1]))
                self.conn.commit() 
        except sqlite3.Error as e:
            print 'get_runtime_params exception %s' % e
            res = (0.001,  random.randint(0, 255))
        finally:
            c.close()
        return res

    def calc_runtime_slowdown(self, dev_path, datatype, target):
        try:
            self.connect()
            c = self.conn.cursor()
            c.execute("SELECT interval, slowdown, parts FROM "
                      "data_crawler_runtime "
                      "WHERE dev_path=? AND datatype=?", (dev_path, datatype))
            res = c.fetchone()
            if res == None:
                new_slowdown = 0.001 
            else:
                interval, slowdown, parts = res
                execution_time =  interval - slowdown * parts
                if execution_time > target:
                    print 'target cycle too small' 
                    new_slowdown = 0.001
                else:
                    new_slowdown = (target - execution_time)/parts
        except sqlite3.Error as e:
            print 'get_runtime_results exception %s' % e
        finally:
            c.close()
        return new_slowdown 

    def get_runtime_results(self):
        try:
            self.connect()
            c = self.conn.cursor()
            for res in c.execute("SELECT * FROM data_crawler_runtime"):
                yield res
        except sqlite3.Error as e:
            print 'get_runtime_results exception %s' % e
        finally:
            c.close()
 
    def set_runtime_results(self, dev_path, datatype, cycle,
                            interval, slowdown, parts):
        try:
            self.connect()
            c = self.conn.cursor()
            c.execute("INSERT OR REPLACE INTO data_crawler_runtime VALUES "
                      "(?, ?, ?, ?, ?, ?, ?)", (dev_path, datatype,
                      cycle, interval, slowdown, parts, time.time()))
            self.conn.commit() 
        except sqlite3.Error as e:
            print 'set_runtime_results exception %s' % e
        finally:
            c.close()


class DataCrawler(threading.Thread):
    """
    A single crawel on a datadir
    Run all necessery tasks
    Pace the Crawel and mesure how much time you sleep
    Mesure the total time 
    Store the runtime results to allow improved pacing in the next run
    """
    def __init__(self, dev_path, datatype, tasks, dirs, conf):
        threading.Thread.__init__(self)
        # DataCrawler will crawl the datatype dir inside dev_path
        self.dev_path = dev_path
        self.datatype = datatype
        # Crawler configuration and plugable services
        self.tasks = tasks
        self.dirs = dirs
        self.conf = conf
        self.cycle_interval = int(conf['crawler'].get('cycle_interval', 0))
 
    def run(self):
        # Open the DB and read the runtime params of this data_crawler_type
        db = CrawlerDb(self.conf)
        self.slowdown, self.cycle = db.get_runtime_params(self.dev_path,
                                                          self.datatype)
        # Automate slowdown calculation
        if self.cycle_interval > 0:
            self.slowdown = db.calc_runtime_slowdown(self.dev_path,
                                                     self.datatype,
                                                     self.cycle_interval)
        # Crawl the datadir once  
        self.slowdown_parts = 0
        start = time.time()
        self.crawl_data()
        interval = time.time() - start
        # Update the DB about the runtime results and stats
        db.set_runtime_results(self.dev_path, self.datatype, self.cycle,
                               interval, self.slowdown, self.slowdown_parts)
        # Add here db.stats(providing stats about the run)

    def slow(self):
        self.slowdown_parts += 1
        time.sleep(self.slowdown)

    def pace(self, section, pset):
        cycles = self.conf[section]['cycles']
        if ((pset % cycles) != (self.cycle % cycles)):
            return False
        return True
       
          
    def crawl_data(self):
        self.slow()
        print 'crawl_data %s ' % self.dev_path
        data_path = os.path.join(self.dev_path, self.dirs.data)
        if not os.path.isdir(data_path):
            return
        if not (self.tasks['partition'] or self.tasks['suffix'] or
                self.tasks['hsh'] or self.tasks['item']):
            return
        partitions = listdir(data_path)
        random.shuffle(partitions)
        for partition in partitions:
            partition_path = os.path.join(data_path, partition)
            print 'crawl_data %s ' % partition_path
            if os.path.isfile(partition_path):
                # Clean up any (probably zero-byte) files where a
                # partition should be.
                self.logger.warning('Removing partition directory '
                                    'which was a file: %s', partition_path)
                os.remove(partition_path)
                continue
            key = md5.new(partition).digest()
            pset = int(unpack_from('>I', key)[0])
            self.crawl_partition(partition_path, partition, pset) 
        
    def crawl_partition(self, partition_path, partition, pset):
        self.slow()
        for section in self.tasks['partition']:
            if self.pace(section, pset):
                self.conf[section]['func'](partition_path, partition)
        if not (self.tasks['partition'] or
                self.tasks['hsh'] or self.tasks['item']):
            return
        for suffix in listdir(partition_path):
            suffix_path = os.path.join(partition_path, suffix)
            if not os.path.isdir(suffix_path):
                continue
            self.crawl_suffix(suffix_path, partition, pset)
 
    def crawl_suffix(self, suffix_path, partition, pset):
        self.slow()
        for section in self.tasks['suffix']:
            if self.pace(section, pset):
                self.conf[section]['func'](suffix_path, partition)
        if not (self.tasks['hsh'] or self.tasks['item']):
            return
        for hsh in listdir(suffix_path):
            hsh_path = os.path.join(suffix_path, hsh)
            if not os.path.isdir(hsh_path):
                continue
            self.crawl_hsh(hsh_path, hsh, partition, pset)

    def crawl_hsh(self, hsh_path, hsh, partition, pset):
        self.slow()
        for section in self.tasks['hsh']:
            if self.pace(section, pset):
                self.conf[section]['func'](hsh_path, partition)
        if not self.tasks['item']:
            return
        if self.datatype == 'object':
            return  # TBD
#            self.crawl_object_hsh(hsh_path, partition)
        else:
            item_path = os.path.join(hsh_path, hsh + '.db')            
            if self.datatype == 'container':
                self.crawl_container_hsh(item_path, hsh, partition, pset)
            elif self.datatype == 'account':
                self.crawl_account_hsh(item_path, hsh, partition, pset)
                
#    def crawl_object_hsh(self, hsh_path, partition):
#        # TBD Read diskFile once here instead of insider the tasks
#        diskFile = DiskFile(filepath)
#        for section in self.tasks['object']['item']:
#            if self.pace(section, pset):
#               self.conf[section]['func'](diskFile, partition)

    def crawl_container_hsh(self, item_path, hsh, partition, pset):
        for section in self.tasks['item']:
            if self.pace(section, pset):
                print 'calling %s' % item_path
                self.conf[section]['func'](item_path)
#
#        # TBD Read broker once here instead of inside the tasks
#        
#        broker = ContainerBroker(filepath)
#        for section in self.tasks['container']['item']:
#            self.conf[section]['func'](broker, hsh, partition)

    def crawl_account_hsh(self, item_path, hsh, partition, pset):
        return
#        # TBD Read broker
#        broker = AccountBroker(filepath)
#        for section in self.tasks['account']['item']:
#            self.conf[section]['func'](broker, hsh partition)
 
if __name__ == "__main__": 
    c = Crawler('/etc/swift/swift-crawler.conf')
    threads = []
    print 'dev_path | datatype | cycle | interval | slowdown | parts | timestamp'
    for res in c.get_past_crawls():
        print res
        threads.append((res[0], res[1])) 
    for (dev_path, datatype) in threads: 
        c.set_crawl(dev_path, datatype, random.random()/10) 
    c.crawl()
    print 'dev_path | datatype | cycle | interval | slowdown | parts | timestamp'

    for res in c.get_past_crawls():
        print res
        execution_time = res[3] - res[5]*res[4]
        new_slowdown = (60 - execution_time)/res[5]
        print "In order for the execution to take 60 seconds " \
              "slowdown should be %s" % new_slowdown
