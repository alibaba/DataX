package cn.com.bluemoon.metadata.base.log;

import cn.hutool.core.date.DateUtil;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/log/BMJobLogger.class */
public class BMJobLogger {
    private static final Logger log = LoggerFactory.getLogger(BMJobLogger.class);

    private static void logDetail(StackTraceElement callInfo, String appendLog) {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append(DateUtil.formatDateTime(new Date())).append(" ").append("[" + Thread.currentThread().getName() + "]").append(" ").append(appendLog != null ? appendLog : "");
        String formatAppendLog = stringBuffer.toString();
        String logFileName = BMFileAppender.contextHolder.get();
        if (logFileName == null || logFileName.trim().length() <= 0) {
            log.info(">>>>>>>>>>> {}", formatAppendLog);
        } else {
            BMFileAppender.appendLog(logFileName, formatAppendLog);
        }
    }

    public static void log(String appendLogPattern, Object... appendLogArguments) {
        logDetail(new Throwable().getStackTrace()[1], MessageFormatter.arrayFormat(appendLogPattern, appendLogArguments).getMessage());
    }

    public static void log(Throwable e) {
        StringWriter stringWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stringWriter));
        logDetail(new Throwable().getStackTrace()[1], stringWriter.toString());
    }
}
