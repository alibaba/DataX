#!/bin/usr/env python
#-*- coding: utf-8 -*-

from optparse import OptionParser
import sys
import json
import tabulate
import zlib
from ots2 import *

class ConsoleConfig:
    def __init__(self, config_file):
        f = open(config_file, 'r')
        config = json.loads(f.read())
        self.endpoint = str(config['endpoint'])
        self.accessid = str(config['accessId'])
        self.accesskey = str(config['accessKey'])
        self.instance_name = str(config['instanceName'])
        self.status_table = str(config['statusTable'])

        self.ots = OTSClient(self.endpoint, self.accessid, self.accesskey, self.instance_name)

def describe_job(config, options):
    '''
        1. get job's description
        2. get all job's checkpoints and check if it is done
    '''
    if not options.stream_id:
        print "Error: Should set the stream id using '-s' or '--streamid'."
        sys.exit(-1)

    if not options.timestamp:
        print "Error: Should set the timestamp using '-t' or '--timestamp'."
        sys.exit(-1)

    pk = [('StreamId', options.stream_id), ('StatusType', 'DataxJobDesc'), ('StatusValue', '%16d' % int(options.timestamp))]
    consumed, pk, attrs, next_token = config.ots.get_row(config.status_table, pk, [], None, 1)
    if not attrs:
        print 'Stream job is not found.'
        sys.exit(-1)

    job_detail = parse_job_detail(attrs)
    print '----------JobDescriptions----------'
    print json.dumps(job_detail, indent=2)
    print '-----------------------------------'

    stream_checkpoints = _list_checkpoints(config, options.stream_id, int(options.timestamp))

    cps_headers = ['ShardId', 'SendRecordCount', 'Checkpoint', 'SkipCount', 'Version']
    table_content = []
    for cp in stream_checkpoints:
        table_content.append([cp['ShardId'], cp['SendRecordCount'], cp['Checkpoint'], cp['SkipCount'], cp['Version']])

    print tabulate.tabulate(table_content, headers=cps_headers)

    # check if stream job has finished
    finished = True
    if len(job_detail['ShardIds']) != len(stream_checkpoints):
        finished = False

    for cp in stream_checkpoints:
        if cp['Version'] != job_detail['Version']:
            finished = False

    print '----------JobSummary----------'
    print 'ShardsCount:', len(job_detail['ShardIds'])
    print 'CheckPointsCount:', len(stream_checkpoints)
    print 'JobStatus:', 'Finished' if finished else 'NotFinished'
    print '------------------------------'

def _list_checkpoints(config, stream_id, timestamp):
    start_pk = [('StreamId', stream_id), ('StatusType', 'CheckpointForDataxReader'), ('StatusValue', '%16d' % timestamp)]
    end_pk = [('StreamId', stream_id), ('StatusType', 'CheckpointForDataxReader'), ('StatusValue', '%16d' % (timestamp + 1))]

    consumed_counter = CapacityUnit(0, 0)
    columns_to_get = []
    checkpoints = []
    range_iter = config.ots.xget_range(
                config.status_table, Direction.FORWARD,
                start_pk, end_pk,
                consumed_counter, columns_to_get, 100,
                column_filter=None, max_version=1
    )

    rows = []
    for (primary_key, attrs) in range_iter:
        checkpoint = {}
        for attr in attrs:
            checkpoint[attr[0]] = attr[1]

        if not checkpoint.has_key('SendRecordCount'):
            checkpoint['SendRecordCount'] = 0
        checkpoint['ShardId'] = primary_key[2][1].split('\t')[1]
        checkpoints.append(checkpoint)

    return checkpoints

def list_job(config, options):
    '''
        Two options:
            1. list all jobs of stream
            2. list all jobs and all streams
    '''
    consumed_counter = CapacityUnit(0, 0)

    if options.stream_id:
        start_pk = [('StreamId', options.stream_id), ('StatusType', INF_MIN), ('StatusValue', INF_MIN)]
        end_pk = [('StreamId', options.stream_id), ('StatusType', INF_MAX), ('StatusValue', INF_MAX)]
    else:
        start_pk = [('StreamId', INF_MIN), ('StatusType', INF_MIN), ('StatusValue', INF_MIN)]
        end_pk = [('StreamId', INF_MAX), ('StatusType', INF_MAX), ('StatusValue', INF_MAX)]

    columns_to_get = []
    range_iter = config.ots.xget_range(
                config.status_table, Direction.FORWARD,
                start_pk, end_pk,
                consumed_counter, columns_to_get, None,
                column_filter=None, max_version=1
    )

    rows = []
    for (primary_key, attrs) in range_iter:
        if primary_key[1][1] == 'DataxJobDesc':
            job_detail = parse_job_detail(attrs)
            rows.append([job_detail['TableName'], job_detail['JobStreamId'], job_detail['EndTime'], job_detail['StartTime'], job_detail['EndTime'], job_detail['Version']])

    headers = ['TableName', 'JobStreamId', 'Timestamp', 'StartTime', 'EndTime', 'Version']
    print tabulate.tabulate(rows, headers=headers)

def parse_job_detail(attrs):
    job_details = {}
    shard_ids_content = ''
    for attr in attrs:
        if attr[0].startswith('ShardIds_'):
            shard_ids_content += attr[1]
        else:
            job_details[attr[0]] = attr[1]

    shard_ids = json.loads(zlib.decompress(shard_ids_content))

    if not job_details.has_key('Version'):
        job_details['Version'] = ''

    if not job_details.has_key('SkipCount'):
        job_details['SkipCount'] = 0
    job_details['ShardIds'] = shard_ids

    return job_details

def parse_time(value):
    try:
        return int(value)
    except Exception,e:
        return int(time.mktime(time.strptime(value, '%Y-%m-%d %H:%M:%S')))

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-c', '--config', dest='config_file', help='path of config file', metavar='tablestore_streamreader_config.json')
    parser.add_option('-a', '--action', dest='action', help='the action to do', choices = ['describe_job', 'list_job'], metavar='')
    parser.add_option('-t', '--timestamp', dest='timestamp', help='the timestamp', metavar='')
    parser.add_option('-s', '--streamid', dest='stream_id', help='the id of stream', metavar='')
    parser.add_option('-d', '--shardid', dest='shard_id', help='the id of shard', metavar='')

    options, args = parser.parse_args()

    if not options.config_file:
        print "Error: Should set the path of config file using '-c' or '--config'."
        sys.exit(-1)

    if not options.action:
        print "Error: Should set the action using '-a' or '--action'."
        sys.exit(-1)

    console_config = ConsoleConfig(options.config_file)
    if options.action == 'list_job':
        list_job(console_config, options)
    elif options.action == 'describe_job':
        describe_job(console_config, options)

