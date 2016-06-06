#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
 Author: deng_lingfei
 Create time: 2016-01-19 14:04
 Last modified: 2016-01-19 14:04
 Filename: client.py
 Description:
'''
import os
import sys

import socket

from mwlogger import MwLogger, UDPHandler

from logging import handlers

import uuid
import traceback
import time


if __name__ == '__main__':
    host="localhost"
    port=9999

    logger = MwLogger("Demo", log_handler=UDPHandler(host, port), use_mwformat=True)

    for i in range(1):

        # task start
        taskid = str(uuid.uuid1())
        logger.event('task', 'task created succ', etype='long', eid=taskid)
        time.sleep(1)

        # gen exception
        try:
            i/0
        except Exception, err:
            logger.event("exception", traceback.format_exc(), errorcode='01019900')
        #time.sleep(10)

        # task end
        # logger.event('task', 'task end', etype='long', eid=taskid, flag='end')

        #time.sleep(1)

