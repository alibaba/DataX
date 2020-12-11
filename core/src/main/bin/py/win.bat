@echo off


SET data_home=%1
SET param2=%2
SET param3=%3
SET param4=%4
SET param5=%5

::字符串拼接(等号前面不能有空格)
SET params=%data_home% %param2% %param3% %param4% %param5%
echo params = %params%

set varA=B
if "%varA%"=="A" (
    echo %varA% is A
    echo AAA
) else if "%varA%"=="B" (
    echo %varA% is B
    echo BBB
) else (
    echo %varA% is C
    echo CCC
)

::java -server -Xms1g -Xmx1g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=D:\idea-workspace\github\DataX\target\datax\datax/log -Xms1g -Xmx1g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=D:\idea-workspace\github\DataX\target\datax\datax/log -Dloglevel=info -Dfile.encoding=UTF-8 -Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener -Djava.security.egd=file:///dev/urandom -Ddatax.home=D:\idea-workspace\github\DataX\target\datax\datax -Dlogback.configurationFile=D:\idea-workspace\github\DataX\target\datax\datax/conf/logback.xml -classpath D:\idea-workspace\github\DataX\target\datax\datax/lib/*  -Dlog.file.name=x\datax\job\job_json com.alibaba.datax.core.Engine -mode standalone -jobid -1 -job D:\idea-workspace\github\DataX\target\datax\datax\job\job.json