#!/usr/bin/env python
# encoding: utf-8

import os
import sys
import time
from datetime import datetime

PROGRAM_INFO = "mw monitor_event_parser 2.15.0.0"

bin_path = os.path.dirname(os.path.abspath(__file__))
app_path = os.path.dirname(bin_path)

lib_path = os.path.join(app_path, 'lib')
etc_path = os.path.join(app_path, 'etc')

sys.path.append(lib_path)
sys.path.append(etc_path)

try:
    from monitor.mwlogger import MwLogger
    from monitor.mwevent import Event
    from monitor.database import connect
    from monitor.mwconfig import Mwconfig
    from monitor.mwemail import sendmail, mailed_cache
except ImportError:
    from mwlogger import MwLogger
    from mwevent import Event
    from database import connect
    from mwconfig import Mwconfig
    from mwemail import sendmail, mailed_cache

try:
    from event_parser_config import *
except ImportError:
    MODULE_NAME = 'mointor_event_parser'
    LOG_FILE = 'syslog'
    LOG_LEVEL = 'INFO'
    DEBUG = False
    EVENT_RESERVE_DAY = 7

now = time.time

if DEBUG:
    logger = MwLogger(MODULE_NAME, log_level='DEBUG')
else:
    logger = MwLogger(MODULE_NAME, LOG_FILE, log_level=LOG_LEVEL)

def need_alarm(events, strategy_config):
    if len(events) == 0:
        return False

    ename = events[0].name
    etype = events[0].type

    strategy = strategy_config.get(etype, {})
    if etype == 'moment':
        return len(events) >= strategy.get(ename, strategy.default)
    else:
        conditions = strategy.get(ename, [])
        return any([len(filter(lambda e: int(now() - e.start_time)
                               > cond['duration'], events)) >= cond['times']
                    for cond in conditions])
    return False


def alarm_event(conf, events):
    '''
    using email to alarm in 2.14.0.0
    others is not implemented
    '''
    if "email" in conf and conf.email:
        email_conf = conf.email
        email_url = email_conf.get("url", "")
        email_tos = email_conf.get("tos", [])
        subject = '{0}-{1}-{2}-{3}'.format(events[0].host,
                events[0].module, events[0].name, events[0].errorcode)
        if events[0].type == 'moment':
            content = events[0].msg
        else:
            content = str([event.event_uuid for event in events])

        if events[0].type == 'moment' and subject in mailed_cache:
            logger.info("this alarm has been send a short time ago, do not email")
        else:
            logger.info("send alarm to {}, subject:{} content:{}".format(email_tos, subject, content))
            sendmail(email_url, email_tos, subject, content)
            if events[0].type == 'moment':
                mailed_cache[subject] = content

    if "sms" in conf and conf.sms:
        pass

    if "weixin" in conf and conf.weixin:
        pass

    if "other" in conf and conf.other:  # other alarm system
        pass

    for event in events:
        event.update2db(alarm_time=int(now()),
                commit=False)
    Event.db_commit()


if __name__ == '__main__':

    if len(sys.argv) > 1:
        print PROGRAM_INFO
        exit(0)

    logger.info("EventParser is running....")

    conf = Mwconfig(os.path.join(etc_path, 'monitor_config.json'))

    try:
        mwconf = Mwconfig(os.path.join(etc_path, 'media_wise.conf'))
    except:
        logger.info("Invalid media wise configure, ignore it")
        mwconf = None

    db_url = conf.db_url
    strategy = conf.strategy

    alarm_conf = conf.alarm

    email_conf = conf.get('email', {})
    email_url = email_conf.get('url', None)
    email_tos = email_conf.get('tos', [])

    logger.info("load config ok!")

    conn = connect(db_url=db_url, charset='utf8', use_unicode=False)
    Event.set_dbconnection(conn)

    # start dbpc thread
    if mwconf and 'dbpc' in mwconf:
        try:
            from dbpc import dbpc
        except:
            logger.warn("load dbpc module failed, ignore it!")
        else:
            dbpc_conf = mwconf.dbpc
            dbpc_sender = dbpc(dbpc_conf.host,
                               int(dbpc_conf.port),
                               dbpc_conf.service,
                               dbpc_conf.component_prefix + MODULE_NAME,
                               logger,
                               int(dbpc_conf.heartbeat_interval))
            dbpc_sender.start()
            logger.info(
                'dbpc thread started. host is %s, port is %s' % (dbpc_conf.host, dbpc_conf.port))

    while True:
        try:
            m_events = Event.loadfromdb(type='moment',
                        alarm_time='0000-00-00 00:00:00',
                        where='UNIX_TIMESTAMP(start_time) > {}'.format(
                                int(now() - conf.scan_range)))
            logger.debug("load moment events: %s" % str(m_events))

            l_events = Event.loadfromdb(alarm_time='0000-00-00 00:00:00',
                    type='long', end_time='0000-00-00 00:00:00')
            logger.debug("load long events: %s" % str(l_events))

            events = m_events + l_events

            g_events = Event.groupbyname(events)
            logger.debug("group by name events is %s" % str(g_events))

            for name, events in g_events.items():
                if need_alarm(events, strategy):
                    logger.info("event[%s] need alarm, start alarm...." % name)
                    try:
                        alarm_event(alarm_conf, events)
                    except Exception, err:
                        logger.error("alarm failed, event name is %s" % name, exc_info=True)
                    else:
                        logger.info("alarm success, event name is %s" % name)

            # delete all normal events and expired events
            try:
                Event.remove(where="UNIX_TIMESTAMP(start_time) < {}".format(
                    now() - EVENT_RESERVE_DAY * 86400))
                Event.remove(type='long', alarm_time='0000-00-00 00:00:00',
                    where="end_time > '0000-00-00 00:00:00'")
            except:
                logger.error("remove expired events from db failed!", exc_info=True)

            time.sleep(conf.scan_interval)

        except:
            logger.error("catch unhandler error, I will exit", exc_info=True)
            # program exit. It's easy to be found
            os._exit(1)

