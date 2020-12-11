#!/usr/bin/env python
# -*- coding:utf-8 -*-

import sys
import os
import signal
import subprocess
import time
import re
import socket
import json
from optparse import OptionParser
from optparse import OptionGroup
from string import Template
import codecs
import platform

def isWindows():
    return platform.system() == 'Windows'

DATAX_HOME = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATAX_VERSION = 'DATAX-OPENSOURCE-3.0'
if isWindows():
    codecs.register(lambda name: name == 'cp65001' and codecs.lookup('utf-8') or None)
    CLASS_PATH = ("%s/lib/*") % (DATAX_HOME)
else:
    CLASS_PATH = ("%s/lib/*:.") % (DATAX_HOME)
LOGBACK_FILE = ("%s/conf/logback.xml") % (DATAX_HOME)
DEFAULT_JVM = "-Xms1g -Xmx1g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%s/log" % (DATAX_HOME)
DEFAULT_PROPERTY_CONF = "-Dfile.encoding=UTF-8 -Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener -Djava.security.egd=file:///dev/urandom -Ddatax.home=%s -Dlogback.configurationFile=%s" % (
    DATAX_HOME, LOGBACK_FILE)
ENGINE_COMMAND = "java -server ${jvm} %s -classpath %s  ${params} com.alibaba.datax.core.Engine -mode ${mode} -jobid ${jobid} -job ${job}" % (
    DEFAULT_PROPERTY_CONF, CLASS_PATH)
REMOTE_DEBUG_CONFIG = "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=9999"

RET_STATE = {
    "KILL": 143,
    "FAIL": -1,
    "OK": 0,
    "RUN": 1,
    "RETRY": 2
}


def getLocalIp():
    try:
        return socket.gethostbyname(socket.getfqdn(socket.gethostname()))
    except:
        return "Unknown"


def suicide(signum, e):
    global child_process
    print >> sys.stderr, "[Error] DataX receive unexpected signal %d, starts to suicide." % (signum)

    if child_process:
        child_process.send_signal(signal.SIGQUIT)
        time.sleep(1)
        child_process.kill()
    print >> sys.stderr, "DataX Process was killed ! you did ?"
    sys.exit(RET_STATE["KILL"])


def register_signal():
    if not isWindows():
        global child_process
        signal.signal(2, suicide)
        signal.signal(3, suicide)
        signal.signal(15, suicide)


def getOptionParser():
    usage = "usage: %prog [options] job-url-or-path"
    parser = OptionParser(usage=usage)

    prodEnvOptionGroup = OptionGroup(parser, "Product Env Options",
                                     "Normal user use these options to set jvm parameters, job runtime mode etc. "
                                     "Make sure these options can be used in Product Env.")
    prodEnvOptionGroup.add_option("-j", "--jvm", metavar="<jvm parameters>", dest="jvmParameters", action="store",
                                  default=DEFAULT_JVM, help="Set jvm parameters if necessary.")
    prodEnvOptionGroup.add_option("--jobid", metavar="<job unique id>", dest="jobid", action="store", default="-1",
                                  help="Set job unique id when running by Distribute/Local Mode.")
    prodEnvOptionGroup.add_option("-m", "--mode", metavar="<job runtime mode>",
                                  action="store", default="standalone",
                                  help="Set job runtime mode such as: standalone, local, distribute. "
                                       "Default mode is standalone.")
    prodEnvOptionGroup.add_option("-p", "--params", metavar="<parameter used in job config>",
                                  action="store", dest="params",
                                  help='Set job parameter, eg: the source tableName you want to set it by command, '
                                       'then you can use like this: -p"-DtableName=your-table-name", '
                                       'if you have mutiple parameters: -p"-DtableName=your-table-name -DcolumnName=your-column-name".'
                                       'Note: you should config in you job tableName with ${tableName}.')
    prodEnvOptionGroup.add_option("-r", "--reader", metavar="<parameter used in view job config[reader] template>",
                                  action="store", dest="reader",type="string",
                                  help='View job config[reader] template, eg: mysqlreader,streamreader')
    prodEnvOptionGroup.add_option("-w", "--writer", metavar="<parameter used in view job config[writer] template>",
                                  action="store", dest="writer",type="string",
                                  help='View job config[writer] template, eg: mysqlwriter,streamwriter')
    parser.add_option_group(prodEnvOptionGroup)

    devEnvOptionGroup = OptionGroup(parser, "Develop/Debug Options",
                                    "Developer use these options to trace more details of DataX.")
    devEnvOptionGroup.add_option("-d", "--debug", dest="remoteDebug", action="store_true",
                                 help="Set to remote debug mode.")
    devEnvOptionGroup.add_option("--loglevel", metavar="<log level>", dest="loglevel", action="store",
                                 default="info", help="Set log level such as: debug, info, all etc.")
    parser.add_option_group(devEnvOptionGroup)
    return parser

def generateJobConfigTemplate(reader, writer):
    readerRef = "Please refer to the %s document:\n     https://github.com/alibaba/DataX/blob/master/%s/doc/%s.md \n" % (reader,reader,reader)
    writerRef = "Please refer to the %s document:\n     https://github.com/alibaba/DataX/blob/master/%s/doc/%s.md \n " % (writer,writer,writer)
    print readerRef
    print writerRef
    jobGuid = 'Please save the following configuration as a json file and  use\n     python {DATAX_HOME}/bin/datax.py {JSON_FILE_NAME}.json \nto run the job.\n'
    print jobGuid
    jobTemplate={
      "job": {
        "setting": {
          "speed": {
            "channel": ""
          }
        },
        "content": [
          {
            "reader": {},
            "writer": {}
          }
        ]
      }
    }
    readerTemplatePath = "%s/plugin/reader/%s/plugin_job_template.json" % (DATAX_HOME,reader)
    writerTemplatePath = "%s/plugin/writer/%s/plugin_job_template.json" % (DATAX_HOME,writer)
    try:
      readerPar = readPluginTemplate(readerTemplatePath);
    except Exception, e:
       print "Read reader[%s] template error: can\'t find file %s" % (reader,readerTemplatePath)
    try:
      writerPar = readPluginTemplate(writerTemplatePath);
    except Exception, e:
      print "Read writer[%s] template error: : can\'t find file %s" % (writer,writerTemplatePath)
    jobTemplate['job']['content'][0]['reader'] = readerPar;
    jobTemplate['job']['content'][0]['writer'] = writerPar;
    print json.dumps(jobTemplate, indent=4, sort_keys=True)

def readPluginTemplate(plugin):
    with open(plugin, 'r') as f:
            return json.load(f)

def isUrl(path):
    if not path:
        return False

    assert (isinstance(path, str))
    m = re.match(r"^http[s]?://\S+\w*", path.lower())
    if m:
        return True
    else:
        return False


def buildStartCommand(options, args):
    commandMap = {}
    tempJVMCommand = DEFAULT_JVM
    if options.jvmParameters:
        tempJVMCommand = tempJVMCommand + " " + options.jvmParameters

    if options.remoteDebug:
        tempJVMCommand = tempJVMCommand + " " + REMOTE_DEBUG_CONFIG
        print 'local ip: ', getLocalIp()

    if options.loglevel:
        tempJVMCommand = tempJVMCommand + " " + ("-Dloglevel=%s" % (options.loglevel))

    if options.mode:
        commandMap["mode"] = options.mode

    # jobResource 可能是 URL，也可能是本地文件路径（相对,绝对）
    jobResource = args[0]
    if not isUrl(jobResource):
        jobResource = os.path.abspath(jobResource)
        if jobResource.lower().startswith("file://"):
            jobResource = jobResource[len("file://"):]

    jobParams = ("-Dlog.file.name=%s") % (jobResource[-20:].replace('/', '_').replace('.', '_'))
    if options.params:
        jobParams = jobParams + " " + options.params

    if options.jobid:
        commandMap["jobid"] = options.jobid

    commandMap["jvm"] = tempJVMCommand
    commandMap["params"] = jobParams
    commandMap["job"] = jobResource

    return Template(ENGINE_COMMAND).substitute(**commandMap)


def printCopyright():
    print '''
DataX (%s), From Alibaba !
Copyright (C) 2010-2017, Alibaba Group. All Rights Reserved.

''' % DATAX_VERSION
    sys.stdout.flush()


if __name__ == "__main__":
    printCopyright()
    parser = getOptionParser()
    options, args = parser.parse_args(sys.argv[1:])
    if options.reader is not None and options.writer is not None:
        generateJobConfigTemplate(options.reader,options.writer)
        sys.exit(RET_STATE['OK'])
    if len(args) != 1:
        parser.print_help()
        sys.exit(RET_STATE['FAIL'])

    startCommand = buildStartCommand(options, args)
    # print startCommand

    child_process = subprocess.Popen(startCommand, shell=True)
    register_signal()
    (stdout, stderr) = child_process.communicate()

    sys.exit(child_process.returncode)
