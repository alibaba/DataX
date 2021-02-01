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


DATAX_HOME = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# print("DATAX_HOME==="+DATAX_HOME)

DATAX_VERSION = 'DATAX-OPENSOURCE-3.0'
if isWindows():
    codecs.register(lambda name: name == 'cp65001' and codecs.lookup('utf-8') or None)
    CLASS_PATH = ("%s/lib/*") % (DATAX_HOME)
else:
    CLASS_PATH = ("%s/lib/*:.") % (DATAX_HOME)

print("CLASS_PATH===" + CLASS_PATH)

LOGBACK_FILE = ("%s/conf/logback.xml") % (DATAX_HOME)
print("LOGBACK_FILE===" + LOGBACK_FILE)

DEFAULT_JVM = "-Xms1g -Xmx1g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%s/log" % (DATAX_HOME)
print("DEFAULT_JVM===" + DEFAULT_JVM)

DEFAULT_PROPERTY_CONF = "-Dfile.encoding=UTF-8 -Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener -Djava.security.egd=file:///dev/urandom -Ddatax.home=%s -Dlogback.configurationFile=%s" % (
    DATAX_HOME, LOGBACK_FILE)
print("DEFAULT_PROPERTY_CONF===" + DEFAULT_PROPERTY_CONF)

ENGINE_COMMAND = "java -server ${jvm} %s -classpath %s  ${params} com.alibaba.datax.core.Engine -mode ${mode} -jobid ${jobid} -job ${job}" % (
    DEFAULT_PROPERTY_CONF, CLASS_PATH)
print("ENGINE_COMMAND===" + ENGINE_COMMAND)

REMOTE_DEBUG_CONFIG = "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=9999"
print("REMOTE_DEBUG_CONFIG===" + REMOTE_DEBUG_CONFIG)

RET_STATE = {
    "KILL": 143,
    "FAIL": -1,
    "OK": 0,  # 无错误退出 不传参数时，默认传0
    "RUN": 1,  # 有错误退出
    "RETRY": 2
}


# 获取本地ip
def getLocalIp():
    try:
        return socket.gethostbyname(socket.getfqdn(socket.gethostname()))
    except:
        return "Unknown"


# 自杀（自己停止程序）
def suicide(signum):
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


# 获取选项解析器
def getOptionParser():
    usage = "usage: %prog [options] job-url-or-path"
    parser = OptionParser(usage)  # 定义解析器

    # 生产环境 配置组
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
                                  action="store", dest="reader", type="string",
                                  help='View job config[reader] template, eg: mysqlreader,streamreader')
    prodEnvOptionGroup.add_option("-w", "--writer", metavar="<parameter used in view job config[writer] template>",
                                  action="store", dest="writer", type="string",
                                  help='View job config[writer] template, eg: mysqlwriter,streamwriter')
    # 将“生产环境配置组”添加到 解析器
    parser.add_option_group(prodEnvOptionGroup)

    # 测试环境 配置组
    devEnvOptionGroup = OptionGroup(parser, "Develop/Debug Options",
                                    "Developer use these options to trace more details of DataX.")
    devEnvOptionGroup.add_option("-d", "--debug", dest="remoteDebug", action="store_true",
                                 help="Set to remote debug mode.")
    devEnvOptionGroup.add_option("--loglevel", metavar="<log level>", dest="loglevel", action="store",
                                 default="info", help="Set log level such as: debug, info, all etc.")
    # 将“测试环境配置组”添加到 解析器
    parser.add_option_group(devEnvOptionGroup)
    return parser


# 生成job配置模板
def generateJobConfigTemplate(reader, writer):
    readerRef = "Please refer to the %s document:\n     https://github.com/alibaba/DataX/blob/master/%s/doc/%s.md \n" % (
        reader, reader, reader)
    writerRef = "Please refer to the %s document:\n     https://github.com/alibaba/DataX/blob/master/%s/doc/%s.md \n " % (
        writer, writer, writer)
    print("readerRef" + readerRef)
    print("writerRef" + writerRef)
    jobGuid = 'Please save the following configuration as a json file and  use\n     python {DATAX_HOME}/bin/datax.py {JSON_FILE_NAME}.json \nto run the job.\n'
    print("jobGuid" + jobGuid)
    jobTemplate = {
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
    readerTemplatePath = "%s/plugin/reader/%s/plugin_job_template.json" % (DATAX_HOME, reader)
    print("readerTemplatePath===" + readerTemplatePath)
    writerTemplatePath = "%s/plugin/writer/%s/plugin_job_template.json" % (DATAX_HOME, writer)
    print("writerTemplatePath===" + writerTemplatePath)
    try:
        readerPar = readPluginTemplate(readerTemplatePath)
    except Exception as e:
        print("Read reader[%s] template error: can\'t find file %s" % (reader, readerTemplatePath))
    try:
        writerPar = readPluginTemplate(writerTemplatePath)
    except Exception as e:
        print("Read writer[%s] template error: : can\'t find file %s" % (writer, writerTemplatePath))
    jobTemplate['job']['content'][0]['reader'] = readerPar
    jobTemplate['job']['content'][0]['writer'] = writerPar
    # 将 Python 对象编码成 JSON 字符串
    #indent 缩进字符，sort_keys 是否排序
    print(json.dumps(jobTemplate, indent=4, sort_keys=True))


# 读取插件模板
def readPluginTemplate(plugin):
    with open(plugin, 'r') as f:
        return json.load(f)


# 输入路径是否是一个url
def isUrl(path):
    if not path:
        return False

    assert (isinstance(path, str))
    m = re.match(r"^http[s]?://\S+\w*", path.lower())
    if m:
        return True
    else:
        return False


# 构建启动命令行
def buildStartCommand(options, args):
    commandMap = {}
    tempJVMCommand = DEFAULT_JVM
    if options.jvmParameters:
        tempJVMCommand = tempJVMCommand + " " + options.jvmParameters

    if options.remoteDebug:
        tempJVMCommand = tempJVMCommand + " " + REMOTE_DEBUG_CONFIG
        print('local ip: ', getLocalIp())

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
    print('''
DataX (%s), From Alibaba !
Copyright (C) 2010-2017, Alibaba Group. All Rights Reserved.
''' % DATAX_VERSION)
    sys.stdout.flush()


if __name__ == "__main__":
    printCopyright()  # 打印版权信息等
    parser = getOptionParser()  # 获取解析器，用于解析datax启动命令里的各项参数
    options, args = parser.parse_args(sys.argv[1:])
    if options.reader is not None and options.writer is not None:
        # 如果参数中有reader和 writer 则 构建job json
        generateJobConfigTemplate(options.reader, options.writer)
        sys.exit(RET_STATE['OK'])
    if len(args) != 1:
        parser.print_help()
        sys.exit(RET_STATE['FAIL'])

    startCommand = buildStartCommand(options, args)
    print("startCommand===" + startCommand)

    child_process = subprocess.Popen(startCommand, True)
    register_signal()
    (stdout, stderr) = child_process.communicate()

    sys.exit(child_process.returncode)
