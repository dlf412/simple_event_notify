#!/usr/bin/env python
# encoding: utf-8

__author__ = 'deng_lingfei'

import re
import simplejson as json
from itertools import groupby


from database import execute, connect


def utf8str(s):
    if isinstance(s, unicode):
        return s.encode('utf8')
    return str(s)


def parse_logging(content):
    '''
    log_format:'%(threadName)s %(asctime)s %(name)s/%(levelname)s/%(filename)s:%(lineno)d:%(funcName)s:%(process)d/%(thread)d:%(message)s'
    return (module, level, msg)
    '''
    r = r'.* (\S*)/(INFO|DEBUG|WARNING|ERROR|CRITICAL).*?:(\{.*\})$'
    m = re.match(r, content)
    if m:
        return m.groups()
    return (None, None, None)


class Base(object):
    # NOTE： db connection maybe lost or gone away, you should catch the
    #exceptions, reconnect and retry
    table = ''
    primary_key = ''
    keys = {}
    conn = None

    @classmethod
    def set_dbconnection(cls, dbconn):
        '''
        the method must be called when you using it
        '''
        cls.conn = dbconn
        cls.conn.ping()

        if len(cls.keys) == 0:
            cls.load_keys()

        if not cls.keys:
            raise Exception("{} is not in database".format(cls.table))

    @classmethod
    def db_commit(cls):
        cls.conn.commit()

    @classmethod
    def db_rollback(cls):
        cls.conn.rollback()

    @classmethod
    def load_keys(cls):
        if cls.table:
            sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='%s'" % cls.table
            _, res = execute(
                sql=sql, conn=cls.conn, commit=True)
            cls.keys = dict(res)   # {column_name: data_type}
            for key, value in cls.keys.items():
                if value == 'timestamp':
                    cls.keys[key] = "from_unixtime(%s)"
                else:
                    cls.keys[key] = "%s"
            del res

    def __init__(self, **args):
        if self.conn is None:
            raise InvalidEvent("please set db_connection first!!")
        if not self.keys:
            self.__class__.load_keys()
            if not self.keys:
                raise Exception("{} is not in database".format(self.table))
        super(Base, self).__setattr__('_obj', args)
        for k, v in args.iteritems():
            setattr(self, k, v)

    def __eq__(self, other):
        return self.event_uuid == other.event_uuid

    def __repr__(self):
        return str(self._obj)

    def __setattr__(self, attr, value):
        self._obj[attr] = value

    def __getattr__(self, attr):
        return self._obj[attr]

    def update2db(self, commit=True, **kwargs):
        assert hasattr(self, self.primary_key)
        if kwargs:
            kw = dict(**kwargs)
        else:
            kw = {key: value for key,
                  value in self._obj.items() if key != self.primary_key}
        sql = "update {0} set {1} where {2}='{3}'".format(self.table,
                                                          ', '.join(["{}={}".format(key, self.keys[key]) for key in kw.keys()]),
                                                          self.primary_key, getattr(self, self.primary_key))
        execute(sql, sql_params=kw.values(), conn=self.conn, commit=commit)


        for k, v in kw.items():
            setattr(self, k, v)


    def save2db(self, commit=True):
        sql = "insert into {0}({1}) values ({2})"
        values = ', '.join([self.keys[key] for key in self._obj.keys()])
        sql = sql.format(self.table, ','.join(self._obj.keys()), values)
        execute(sql, sql_params=self._obj.values(),
                conn=self.conn, commit=commit)

    def indb(self):
        return self.__class__.exists(event_uuid=self.event_uuid)

    def removefromdb(self):
        return self.__class__.remove(event_uuid=self.event_uuid)

    def reload(self):
        assert hasattr(self, self.primary_key)
        keys = ["unix_timestamp(%s)" % key if self.keys[key] == 'from_unixtime(%s)' else key for key in self.keys.keys()]
        sql = 'select %s from %s %s' % (','.join(keys), self.table, "where %s='%s'" % (
            self.primary_key, getattr(self, self.primary_key)))
        _, res = execute(sql, conn=self.conn, commit=True)
        self._obj.update(dict(zip(self.keys.keys(), res[0])))

    @staticmethod
    def condition(where, **kwargs):
        # cond = ' and '.join(["%s='%s'" % (key, value)
        #                     for key, value in kwargs.items()])

        cond = ' and '.join(["%s=%%s" % key for key in kwargs.keys()])
        if where and cond:
            cond = '(%s) and ' % where + cond
        elif where:
            cond = where
        if cond:
            cond = ' where ' + cond
        return cond

    @classmethod
    def loadfromdb(cls, where='', **kwargs):
        condition = cls.condition(where, **kwargs)
        keys = ["unix_timestamp(%s)" % key if cls.keys[key] == 'from_unixtime(%s)' else key for key in cls.keys.keys()]
        sql = 'select {0} from {1} {2}'.format(
            ','.join(keys), cls.table, condition)
        # sql = 'select %s from %s %s' % (
        #    ','.join(cls.keys), cls.table, condition)
        _, res = execute(
            sql, sql_params=dict(**kwargs).values(), conn=cls.conn, commit=True)
        return [cls(**dict(zip(cls.keys.keys(), r))) for r in res]

    @classmethod
    def count(cls, where='', **kwargs):
        condition = cls.condition(where, **kwargs)
        sql = 'select count(1) from {0} {1}'.format(cls.table, condition)
        _, res = execute(
            sql, sql_params=dict(**kwargs).values(), conn=cls.conn, commit=True)
        return res[0][0]

    @classmethod
    def exists(cls, where='', **kwargs):
        condition = cls.condition(where, **kwargs)
        sql = 'select count(1) from %s %s' % (cls.table, condition)
        _, res = execute(
            sql, sql_params=dict(**kwargs).values(), conn=cls.conn, commit=True)
        return res[0][0] > 0

    @classmethod
    def remove(cls, where='', **kwargs):
        condition = cls.condition(where, **kwargs)
        sql = 'delete from {0} {1}'.format(cls.table, condition)
        ret, _ = execute(
            sql, sql_params=dict(**kwargs).values(), conn=cls.conn, commit=True)
        return ret


class NotEvent(Exception):
    pass


class InvalidEvent(Exception):
    pass


class Errormsg(Base):
    '''
    CREATE TABLE `error_msg` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `msg` varchar(3000) NOT NULL DEFAULT '' COMMENT 'event message',
    PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    '''
    table = 'error_msg'
    primary_key = 'id'

class Event(Base):

    '''
    type: long/moment
    id:
    name:
    start_time, end_time: timestamp
    host:
    module:
    level: ERROR/WARNING/CRITICAL
    msg:
    '''
    table = 'mw_event'
    primary_key = 'event_uuid'


    @classmethod
    def loadfromlog(cls, host, log):
        obj = {}
        obj['host'] = host
        (obj['module'], obj['level'], log_msg) = parse_logging(log)
        if log_msg:
            jmsg = json.loads(log_msg)
            _event = jmsg.get('event', {})
            obj['msg'] = jmsg.get('msg', "")[:3000] # FIXME database filed set maxsize is 3k, should read from schema
            obj['errorcode'] = jmsg.get('errorcode', "")
            if not _event:
                raise NotEvent
            try:
                obj.update(_event)
                for key in ('event_uuid', 'name', 'type'):
                    if key not in obj:
                        raise InvalidEvent('%s not in event' % key)
                return cls(**obj)
            except Exception, err:
                raise InvalidEvent(err)

    @classmethod
    def groupbyname(cls, events):
        try:
            if isinstance(events, cls):
                return {events.name: [events]}
            elif isinstance(events, list):
                events.sort(key=lambda e: e.name)
                grouper = groupby(events, key=lambda e: e.name)
                return {key: list(g) for key, g in grouper}
            else:
                raise InvalidEvent
        except:
            raise InvalidEvent


if __name__ == '__main__':

    log_format = '%(threadName)s %(asctime)s %(name)s/%(levelname)s/%(filename)s:%(lineno)d:%(funcName)s:%(process)d/%(thread)d:%(message)s'
    create_table_sql = ''' CREATE TABLE `mw_event` (
  `id` varchar(40) NOT NULL COMMENT 'event id',
  `name` varchar(40) NOT NULL COMMENT 'event name',
  `type` enum('long','moment') NOT NULL DEFAULT 'moment' COMMENT '类型: long/moment',
  `start_time` int(10) NOT NULL COMMENT '开始时间',
  `end_time` int(10) DEFAULT '0' COMMENT '结束时间, 如果type为moment此时间无意义',
  `host` varchar(30) NOT NULL DEFAULT '' COMMENT 'event产生的主机',
  `module` varchar(30) NOT NULL DEFAULT '' COMMENT 'event产生的模块',
  `level` enum('ERROR','WARNING','CRITICAL','INFO','DEBUG') NOT NULL DEFAULT 'ERROR' COMMENT 'event日志级别',
  `errorcode` varchar(20) NOT NULL DEFAULT '' COMMENT 'moment event错误代码',
  `msg` text COMMENT 'event日志内容',
  PRIMARY KEY (`id`),
  KEY `idx_name` (`name`),
  KEY `idx_start_time` (`start_time`),
  KEY `idx_end_time` (`end_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
    '''
    drop_table_sql = "drop table if exists mwevent;"

    tt = '''MainThread 2016-02-15 14:57:57,809 test_log/INFO/mwlogger.py:208:<module>:21720/-1221219584:{"msg": "task created succ!!, %@^;'你好的健康的'hihi#()//&*", "company_id": "123", "event": {"start_time": 1455519478, "type": "long", "name": "task", "event_uuid": "ieeuuriir-iekkffe-eiei3933-d3i3i3i"}}'''
    not_event_tt = '''MainThread 2016-02-15 10:17:32,579 test_log/INFO/mwlogger.py:201:<module>:11792/-1221629184:{"msg": "task created succ!!", "company_id": "123", "event_not":{"flag": "start", "time": 1455502652.579617, "type": "long", "name": "task", "event_uuid": "ieeuuriir-iekkffe-eiei3933-d3i3i3i"}}'''
    invalid_event_tt = '''MainThread 2016-02-15 10:17:32,579 test_log/INFO/mwlogger.py:201:<module>:11792/-1221629184:{"msg": "task created succ!!", "company_id": "123", "event":{"flag": "start", "time": 1455502652.579617, "type": "long", "event_uuid": "ieeuuriir-iekkffe-eiei3933-d3i3i3i"}}'''

    conn = connect(
        db_url="mysql://root:123@127.0.0.1/test", charset='utf8', use_unicode=False)
    Event.set_dbconnection(conn)

    Event.remove()

    event = Event.loadfromlog('127.0.0.1', tt)

    if not event.indb():
        event.save2db()
        #if reload, the PRIMARY key id be loaded, event_uuid can not be changed!
        #event.reload()

    levent = Event.loadfromdb(event_uuid='ieeuuriir-iekkffe-eiei3933-d3i3i3i')[0]

    print levent

    # levent include PRIMARY id
    assert levent == event

    event.event_uuid = "120302-209294-02993-iiie0233"
    if not event.indb():
        event.save2db()


    import time
    #levent.start_time = datetime.fromtimestamp(int(time.time()))
    levent.start_time = int(time.time())
    levent.name = "IOError"
    levent.type = "moment"
    levent.alarm_time = int(time.time())
    levent.end_time = int(time.time())
    levent.update2db()


    print "============"
    es = Event.loadfromdb()

    assert es == [levent, event]

    ge = [levent, event, levent, event, levent, event]

    assert Event.groupbyname(ge) == {levent.name: [levent, levent, levent],
                                     event.name: [event, event, event]}
    assert Event.groupbyname(levent) == {levent.name: [levent]}

    event.end_time = 0

    print event

    event.update2db()
    print "============"

    assert [levent] == Event.loadfromdb(
        type='moment', where='UNIX_TIMESTAMP(start_time) > %d' % int(time.time() - 600))
    assert [event] == Event.loadfromdb(type='long', end_time=0)

    assert Event.count() == 2

    event.removefromdb()
    levent.removefromdb()

    assert event.loadfromdb() == []

    # test not_event
    try:
        not_event = Event.loadfromlog("127.0.0.1", not_event_tt)
    except Exception, err:
        assert isinstance(err, NotEvent)

    # test invalid event
    try:
        invalid_event = Event.loadfromlog("127.0.0.1", invalid_event_tt)
    except Exception, err:
        assert isinstance(err, InvalidEvent)

    print "test OK!!!"

