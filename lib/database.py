#!/usr/bin/env python
# encoding: utf-8

'''Database wrapper around MySQLdb'''

import MySQLdb
import os
import sys
import urllib

__filedir__ = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, __filedir__)


__all__ = [
    'connect', 'execute',
]


def parse_url(url, default_port=None):
    '''
    Parse url in the following form:
      PROTO://[USER:[:PASSWD]@]HOST[:PORT][/PATH[;ATTR][?QUERY]]
    A tuple containing (proto, user, passwd, host, port, path, tag, attrs, query) is returned,
    where `attrs' is a tuple containing ('attr1=value1', 'attr2=value2', ...)
    '''
    proto, user, passwd, host, port, path, tag, attrs, query = (None, ) * 9

    try:
        proto, tmp_host = urllib.splittype(url)
        tmp_host, tmp_path = urllib.splithost(tmp_host)
        tmp_user, tmp_host = urllib.splituser(tmp_host)
        if tmp_user:
            user, passwd = urllib.splitpasswd(tmp_user)
        host, port = urllib.splitport(tmp_host)
        port = int(port) if port else default_port
        tmp_path, query = urllib.splitquery(tmp_path)
        tmp_path, attrs = urllib.splitattr(tmp_path)
        path, tag = urllib.splittag(tmp_path)
    except Exception, err:
        raise Exception('parse_db_url error - {0}'.format(str(err)))

    return proto, user, passwd, host, port, path, tag, attrs, query


def connect(db_url=None, **kwargs):
    '''
    Connect database whether using a db_url, or normal host-port params.
    An connection is returned on success, otherwise exception is raised

    Examples:
        conn = connect(db_url='mysql://vdna:123456@127.0.0.1/mddb_local')
      or
        conn = connect(host='127.0.0.1', port=3306, user='vdna',
                       passwd='123456', db='mddb_local', use_unicode=True)
    '''
    if db_url:
        proto, user, passwd, host, port, db, \
            table = parse_db_url(db_url,
                                 default_port=3306)
        if proto != 'mysql':
            raise Exception('protocol not supported - {0}'.format(proto))
        return MySQLdb.connect(host=host, port=port, user=user, passwd=passwd,
                               db=db, **kwargs)
    else:
        return MySQLdb.connect(**kwargs)


def execute(sql, sql_params=None, curs=None, conn=None, commit=False, **kwargs):
    '''
    Execute on sql on the specified cursor `curs', or connection `conn', or
    create a new connection using `kwargs'.

    Examples:
        conn = connect('mysql://vdna:123456@127.0.0.1/mddb_local')
        ret1, res1 = execute(sql1, sql_params1, conn=conn)
        ret2, res2 = execute(sql2, sql_params2, conn=conn)
        ...
        conn.commit()
      or
        ret, res = execute(sql, sql_params, commit=True,
                           db_url='mysql://vdna:123456@127.0.0.1/mddb_local')
      or
        db_conf = {'host': '127.0.0.1', 'port': 3306, 'user': 'vdna',
                   'passwd': '123456', 'db': 'mddb_local'}
        ret, res = execute(sql, sql_params, commit=True, **db_conf)
    '''
    curt = curs
    cont = conn
    if not curt:
        if not cont:
            cont = connect(**kwargs)
        curt = cont.cursor()
    if isinstance(sql, unicode):
        sql = sql.encode('utf-8')
    try:
        ret = curt.execute(sql, sql_params)
    except MySQLdb.OperationalError, err:
        if err.args[0] in (2003, 2006, 2013):
            cont.ping(True)
            ret = curt.execute(sql, sql_params)
    res = curt.fetchall()
    if commit:
        try:
            cont.commit()
        except:
            cont.rollback()
            raise
    # Internally created connection, close when done
    if not curs and not conn:
        cont.close()

    return ret, res


def parse_db_url(db_url, default_port=None):
    '''
    Parse an url representation of one database settings.
    The `db_url' is in the following form:
      PROTO://[USER[:PASSWD]@]HOST[:PORT][/DB/TABLE]
    Tuple (proto, user, passwd, host, port, db, table) is returned
    '''
    proto, user, passwd, host, port, db, table = (None, ) * 7

    try:
        proto, user, passwd, host, port, path = parse_url(db_url,
                                                          default_port)[0:6]
        if not passwd:
            passwd = ''
        tmp_list = path.split('/')[1:]
        db, table = '', ''
        if len(tmp_list) >= 2:
            db, table = tmp_list[0:2]
        elif len(tmp_list) == 1:
            db = tmp_list[0]
    except Exception, err:
        raise Exception('parse_db_url error - {0}'.format(str(err)))

    return proto, user, passwd, host, port, db, table

if __name__ == '__main__':
    import time
    #sql = 'select distinct channel_uuid from channel'

    #ret, res = execute(sql=sql, **db_conf)
    # print ret, res
    # time.sleep(1)
    #db_conf = {'user': 'root'}
    #ret, res = execute(sql='select unix_timestamp()', **db_conf)
    # print ret, res
    # time.sleep(1)
    conn = connect(
        db_url='mysql://root:123@127.0.0.1/test',  charset='utf8', use_unicode=False)
    ret, res = execute(
        sql="SELECT column_name, data_type FROM information_schema.columns WHERE table_name='Batch'", conn=conn)
    print ret, res

    columns = [r[0] for r in res]

    cols = ','.join(columns)

    sql = "select %s from Batch" % cols

    ret, res = execute(sql=sql, conn=conn)
    print ret, res

    print type(res[0][8])

    #ret, res = execute(sql='delete from task_priority_config', commit=True, conn=conn)
    #ret, res = execute(sql='insert into task_priority_config(keyword) values ("世界")', conn=conn)
    # conn.commit()
    # print ret, res
    #ret, res = execute(sql='select * from task_priority_config', conn=conn)
    # print ret, res, res[0][1]

    # print res[0][1] == "世界"

    #ret, res = execute(sql='insert into task_priority_config(keyword) values ("%s")' % res[0][1] , conn=conn)
    # conn.commit()
    # print ret, res
