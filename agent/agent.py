#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
 Author: deng_lingfei
 Create time: 2016-01-19 13:36
 Last modified: 2016-01-19 13:36
 Filename: main_server.py
 Description:
'''
import os
import sys
from SocketServer import ThreadingUDPServer
from SocketServer import BaseRequestHandler
import logging
from logging import handlers
import threading
import Queue
import time

PROGRAM_INFO = "mw monitor_agent 2.15.0.0"

bin_path = os.path.dirname(os.path.abspath(__file__))
app_path = os.path.dirname(bin_path)

lib_path = os.path.join(app_path, 'lib')
etc_path = os.path.join(app_path, 'etc')

sys.path.append(lib_path)
sys.path.append(etc_path)

try:
    from monitor.mwlogger import MwLogger
    from monitor.mwevent import Event, InvalidEvent, NotEvent
    from monitor.database import connect
    from monitor.mwconfig import Mwconfig
except ImportError:
    from mwlogger import MwLogger
    from mwevent import Event, InvalidEvent, NotEvent
    from database import connect
    from mwconfig import Mwconfig

try:
    from agent_config import *
except ImportError:
    MODULE_NAME = 'monitor_agent'
    CACHE_SIZE = 1024000
    READ_CACHE_TIMEOUT = 10
    SAVE_INTERVAL = 30
    MONITOR_LOG_FILE = 'var/log/monitor.log'
    MONITOR_LOG_BACKUP_DAY = 7
    LOG_FILE = 'syslog'
    LOG_LEVEL = 'INFO'
    DEBUG = False

def create_monitor_logger():
    MONITOR_LOG = os.path.join(app_path, MONITOR_LOG_FILE)
    log_dir = os.path.dirname(MONITOR_LOG)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_handler = handlers.TimedRotatingFileHandler(MONITOR_LOG, when='d', interval=1,
                                                    backupCount=MONITOR_LOG_BACKUP_DAY, encoding=None, delay=False, utc=True)

    logger = MwLogger("monitor", log_handler=log_handler, use_mwformat=False)
    if DEBUG:
        logger.addHandler(logging.StreamHandler(sys.stdout))
    return logger

monitor_logger = create_monitor_logger()

if DEBUG:
    # stream handler
    logger = MwLogger(MODULE_NAME, log_level='DEBUG')
else:
    # syslog handler
    logger = MwLogger(MODULE_NAME, LOG_FILE, log_level=LOG_LEVEL)

cacher = Queue.Queue(CACHE_SIZE)
now = time.time
calue_save_time = lambda: now() + SAVE_INTERVAL

def saveEvents(events):
    try:
        try:
            if len(events) == 0:
                return
            for event in events:
                if event.indb():
                    event.update2db(commit=False)
                else:
                    event.save2db(commit=False)
            Event.db_commit()
            # clear events after saving events ok
            del events[:]
        except Exception, error:
            logger.error("save Events error: %s" % str(error), exc_info=True)
            Event.db_rollback()
            time.sleep(1)
    except:
        logger.error("can't save Event to db. db maybe gone away", exc_info=True)


# the thread should run forevery
def event_saver():
    COMMIT_COUNT = 1000
    events = []
    next_save_time = calue_save_time()
    while True:
        try:
            events.append(cacher.get(block=True, timeout=READ_CACHE_TIMEOUT))
        except Queue.Empty:
            logger.info("read cache timeout, trigger save events")
            logger.debug("save events:%s" % str(events))
            saveEvents(events)
        except:
            logger.error("thread run error, I will exist", exc_info=True)
            os._exit(1)
        else:
            if len(events) >= COMMIT_COUNT or now() >= next_save_time:
                logger.info("save events, event count is %d" % len(events))
                saveEvents(events)
        next_save_time = calue_save_time()

class RequestHandler(BaseRequestHandler):

    def handle(self):
        msg = self.request[0]
        monitor_logger.info("[%s]:%s" % (self.client_address[0], msg))
        # parse msg and get event
        try:
            event = Event.loadfromlog(self.client_address[0], msg)
            logger.debug(str(event))
        except NotEvent:
            pass
        except InvalidEvent:
            logger.warn("Invalid Event[%s]" % msg, exc_info=True)
        except:
            logger.error("handle request[%s] error" % msg, exc_info=True)
        else:
            logger.debug(str(event))
            try:
                cacher.put_nowait(event)
            except Queue.Full:
                logger.info("cache is full, save event directly!")
                saveEvents([event])

if __name__ == '__main__':

    if len(sys.argv) > 1:
        print PROGRAM_INFO
        exit(0)

    logger.info('Agent is running....')

    myconf = Mwconfig(os.path.join(etc_path, 'monitor_config.json'))

    try:
        mwconf = Mwconfig(os.path.join(etc_path, 'media_wise.conf'))
    except:
        logger.info("media wise config invalid, ignore it")
        mwconf = None


    bind_host = myconf.get('agent_bind', {}).get('host', '0.0.0.0')
    bind_port = myconf.get('agent_bind', {}).get('port', 9999)

    db_url = myconf.db_url
    conn = connect(db_url=db_url, charset='utf8', use_unicode=False)
    Event.set_dbconnection(conn)
    logger.info("connect db ok, dburl is %s" % db_url)

    ## start dbpc thread
    if mwconf and 'dbpc' in mwconf:
        try:
            from dbpc import dbpc
        except:
            logger.warn("load dbpc module failed, ignore it!")
        else:
            conf = mwconf.dbpc
            dbpc_sender = dbpc(conf.host,
                            int(conf.port),
                            conf.service,
                            conf.component_prefix + MODULE_NAME,
                            logger,
                            int(conf.heartbeat_interval))
            dbpc_sender.start()
            logger.info('dbpc thread started. host is %s, port is %s' % (conf.host, conf.port))

    try:
        saver = threading.Thread(target=event_saver)
        saver.setDaemon(True)
        saver.start()
        logger.info('cache saver thread running....')

        logger.info("UDP Server start, host is %s, port is %d" %
                      (bind_host, bind_port))
        server = ThreadingUDPServer((bind_host, bind_port), RequestHandler)
        server.serve_forever()

        saver.join()
    except:
        logger.error(
            "catch unhandler exception, I will exit",  exc_info=True)
        # program exit. It's easy to be found
        time.sleep(1)
        os._exit(1)

