@echo off


SET param1=%1
SET param2=%2
SET param3=%3
SET param4=%4

SET current_path=%cd%

cd ..
set datax_home=%cd%
set log_path=%datax_home%"/log"
set logback_path=%datax_home%"/conf/logback.xml"
set lib_path=%datax_home%"/lib/*"
cd bin
::echo %log_path%"--------"%log_path%"------"%logback_path%
set /p value=<D:\idea-workspace\github\DataX\target\datax\datax/plugin/reader/streamreader/plugin_job_template.json
echo %datax_home%

::拼接参数(符串拼接等号前面不能有空格)
SET params=%param1% %param2% %param3% %param4%
echo params = %params%

set json_param=.json
::判断第一个参数是不是json文件
echo %param1% | findstr %json_param% >nul && (
	echo === start datax task ===
	java -server -Xms1g -Xmx1g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%log_path% -Xms1g -Xmx1g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%log_path% -Dloglevel=info -Dfile.encoding=UTF-8 -Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener -Djava.security.egd=file:///dev/urandom -Ddatax.home=%datax_home% -Dlogback.configurationFile=%logback_path% -classpath %lib_path%  -Dlog.file.name=x\datax\job\job_json com.alibaba.datax.core.Engine -mode standalone -jobid -1 -job %param1%
) || (

	echo ======get datax config======
	set readerTemplatePath=%datax_home%/plugin/reader/%param2%/plugin_job_template.json
    echo %readerTemplatePath%
	set reader_json=
	for /f "delims=" %%i in (%readerTemplatePath%) do (
		call set "reader_json=%%reader_json%%%%i"
	)
	echo "reader_json====="%reader_json%

    set writerTemplatePath=%datax_home%/plugin/writer/%param4%/plugin_job_template.json
    echo %writerTemplatePath%
	set writer_json=
	for /f "delims=" %%i in (%writerTemplatePath%) do (
		call set "writer_json=%%writer_json%%%%i"
	)
	echo "writer_json====="%writer_json%

	set template_json1={"job":{"setting":{"speed":{"channel":""}},"content":[{"reader":{
	set template_json2=},"writer":{
	set template_json3=}}]}}

	echo -----------------
	echo %template_json1%%reader_json%%template_json2%%writer_json%%template_json3%

)