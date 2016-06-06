#!/usr/bin/env python
# encoding: utf-8

MODULE_NAME = 'monitor_agent'

CACHE_SIZE = 1024*1024
READ_CACHE_TIMEOUT = 10
SAVE_INTERVAL = 30

# MONITOR_LOG only record the request of clients
MONITOR_LOG_FILE = 'var/log/monitor.log'  # abspath is $app/var/log/monitor.log
MONITOR_LOG_BACKUP_DAY = 7

# The log records running info into syslog
LOG_FILE = 'syslog'
LOG_LEVEL = 'INFO'

# the log_level will be set DEBUG, and output is stdout if DEBUG equal True
DEBUG = False

