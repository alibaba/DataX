#! /usr/bin/env python
# vim: set expandtab tabstop=4 shiftwidth=4 foldmethod=marker nu:

import re
import sys
import time

REG_SQL_WAKE = re.compile(r'Begin\s+to\s+read\s+record\s+by\s+Sql', re.IGNORECASE)
REG_SQL_DONE = re.compile(r'Finished\s+read\s+record\s+by\s+Sql', re.IGNORECASE)
REG_SQL_PATH = re.compile(r'from\s+(\w+)(\s+where|\s*$)', re.IGNORECASE)
REG_SQL_JDBC = re.compile(r'jdbcUrl:\s*\[(.+?)\]', re.IGNORECASE)
REG_SQL_UUID = re.compile(r'(\d+\-)+reader')
REG_COMMIT_UUID = re.compile(r'(\d+\-)+writer')
REG_COMMIT_WAKE = re.compile(r'begin\s+to\s+commit\s+blocks', re.IGNORECASE)
REG_COMMIT_DONE = re.compile(r'commit\s+blocks\s+ok', re.IGNORECASE)

# {{{ function parse_timestamp() #
def parse_timestamp(line):
    try:
        ts = int(time.mktime(time.strptime(line[0:19], '%Y-%m-%d %H:%M:%S')))
    except:
        ts = 0

    return ts

# }}} #

# {{{ function parse_query_host() #
def parse_query_host(line):
    ori = REG_SQL_JDBC.search(line)
    if (not ori):
        return ''

    ori = ori.group(1).split('?')[0]
    off = ori.find('@')
    if (off > -1):
        ori = ori[off+1:len(ori)]
    else:
        off = ori.find('//')
        if (off > -1):
            ori = ori[off+2:len(ori)]

    return ori.lower()
# }}} #

# {{{ function parse_query_table() #
def parse_query_table(line):
    ori = REG_SQL_PATH.search(line)
    return (ori and ori.group(1).lower()) or ''
# }}} #

# {{{ function parse_reader_task() #
def parse_task(fname):
    global LAST_SQL_UUID
    global LAST_COMMIT_UUID
    global DATAX_JOBDICT
    global DATAX_JOBDICT_COMMIT
    global UNIXTIME
    LAST_SQL_UUID = ''
    DATAX_JOBDICT = {}
    LAST_COMMIT_UUID = ''
    DATAX_JOBDICT_COMMIT = {}

    UNIXTIME = int(time.time())
    with open(fname, 'r') as f:
        for line in f.readlines():
            line = line.strip()

            if (LAST_SQL_UUID and (LAST_SQL_UUID in DATAX_JOBDICT)):
                DATAX_JOBDICT[LAST_SQL_UUID]['host'] = parse_query_host(line)
                LAST_SQL_UUID = ''

            if line.find('CommonRdbmsReader$Task') > 0:
                parse_read_task(line)
            elif line.find('commit blocks') > 0:
                parse_write_task(line)
            else:
                continue
# }}} #

# {{{ function parse_read_task() #
def parse_read_task(line):
    ser = REG_SQL_UUID.search(line)
    if not ser:
        return

    LAST_SQL_UUID = ser.group()
    if REG_SQL_WAKE.search(line):
        DATAX_JOBDICT[LAST_SQL_UUID] = {
            'stat' : 'R',
            'wake' : parse_timestamp(line),
            'done' : UNIXTIME,
            'host' : parse_query_host(line),
            'path' : parse_query_table(line)
        }
    elif ((LAST_SQL_UUID in DATAX_JOBDICT) and REG_SQL_DONE.search(line)):
        DATAX_JOBDICT[LAST_SQL_UUID]['stat'] = 'D'
        DATAX_JOBDICT[LAST_SQL_UUID]['done'] = parse_timestamp(line)
# }}} #

# {{{ function parse_write_task() #
def parse_write_task(line):
    ser = REG_COMMIT_UUID.search(line)
    if not ser:
        return

    LAST_COMMIT_UUID = ser.group()
    if REG_COMMIT_WAKE.search(line):
        DATAX_JOBDICT_COMMIT[LAST_COMMIT_UUID] = {
            'stat' : 'R',
            'wake' : parse_timestamp(line),
            'done' : UNIXTIME,
        }
    elif ((LAST_COMMIT_UUID in DATAX_JOBDICT_COMMIT) and REG_COMMIT_DONE.search(line)):
        DATAX_JOBDICT_COMMIT[LAST_COMMIT_UUID]['stat'] = 'D'
        DATAX_JOBDICT_COMMIT[LAST_COMMIT_UUID]['done'] = parse_timestamp(line)
# }}} #

# {{{ function result_analyse() #
def result_analyse():
    def compare(a, b):
        return b['cost'] - a['cost']

    tasklist = []
    hostsmap = {}
    statvars = {'sum' : 0, 'cnt' : 0, 'svr' : 0, 'max' : 0, 'min' : int(time.time())}
    tasklist_commit = []
    statvars_commit = {'sum' : 0, 'cnt' : 0}

    for idx in DATAX_JOBDICT:
        item = DATAX_JOBDICT[idx]
        item['uuid'] = idx;
        item['cost'] = item['done'] - item['wake']
        tasklist.append(item);

        if (not (item['host'] in hostsmap)):
            hostsmap[item['host']] = 1
            statvars['svr'] += 1

        if (item['cost'] > -1 and item['cost'] < 864000):
            statvars['sum'] += item['cost']
            statvars['cnt'] += 1
            statvars['max'] = max(statvars['max'], item['done'])
            statvars['min'] = min(statvars['min'], item['wake'])

    for idx in DATAX_JOBDICT_COMMIT:
        itemc = DATAX_JOBDICT_COMMIT[idx]
        itemc['uuid'] = idx
        itemc['cost'] = itemc['done'] - itemc['wake']
        tasklist_commit.append(itemc)

        if (itemc['cost'] > -1 and itemc['cost'] < 864000):
            statvars_commit['sum'] += itemc['cost']
            statvars_commit['cnt'] += 1

    ttl = (statvars['max'] - statvars['min']) or 1
    idx = float(statvars['cnt']) / (statvars['sum'] or ttl)

    tasklist.sort(compare)
    for item in tasklist:
        print '%s\t%s.%s\t%s\t%s\t% 4d\t% 2.1f%%\t% .2f' %(item['stat'], item['host'], item['path'],
                                                           time.strftime('%H:%M:%S', time.localtime(item['wake'])),
                                                           (('D' == item['stat']) and time.strftime('%H:%M:%S', time.localtime(item['done']))) or '--',
                                                           item['cost'], 100 * item['cost'] / ttl, idx * item['cost'])

    if (not len(tasklist) or not statvars['cnt']):
        return

    print '\n--- DataX Profiling Statistics ---'
    print '%d task(s) on %d server(s), Total elapsed %d second(s), %.2f second(s) per task in average' %(statvars['cnt'],
                                                                                                         statvars['svr'], statvars['sum'], float(statvars['sum']) / statvars['cnt'])
    print 'Actually cost %d second(s) (%s - %s), task concurrency: %.2f, tilt index: %.2f' %(ttl,
                                                                                             time.strftime('%H:%M:%S', time.localtime(statvars['min'])),
                                                                                             time.strftime('%H:%M:%S', time.localtime(statvars['max'])),
                                                                                             float(statvars['sum']) / ttl, idx * tasklist[0]['cost'])

    idx_commit = float(statvars_commit['cnt']) / (statvars_commit['sum'] or ttl)
    tasklist_commit.sort(compare)
    print '%d task(s) done odps comit, Total elapsed %d second(s), %.2f second(s) per task in average, tilt index: %.2f' % (
        statvars_commit['cnt'],
        statvars_commit['sum'], float(statvars_commit['sum']) / statvars_commit['cnt'],
        idx_commit * tasklist_commit[0]['cost'])

# }}} #

if (len(sys.argv) < 2):
    print "Usage: %s filename" %(sys.argv[0])
    quit(1)
else:
    parse_task(sys.argv[1])
    result_analyse()