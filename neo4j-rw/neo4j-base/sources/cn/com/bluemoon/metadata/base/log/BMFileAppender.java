package cn.com.bluemoon.metadata.base.log;

import cn.com.bluemoon.metadata.common.utils.ApplicationContextHolder;
import cn.com.bluemoon.metadata.inter.dto.out.OfflineLogResult;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/log/BMFileAppender.class */
public class BMFileAppender {
    private static final Logger log = LoggerFactory.getLogger(BMFileAppender.class);
    public static final InheritableThreadLocal<String> contextHolder = new InheritableThreadLocal<>();
    private static String logBasePath = null;
    private static String configFilePath = null;

    public static String getConfigFilePath() {
        return configFilePath;
    }

    public static void setConfigFilePath(String configFilePath2) {
        configFilePath = configFilePath2;
    }

    public static void setLogBasePath(String logBasePath2) {
        logBasePath = logBasePath2;
    }

    public static void initLogPath(String logPath) {
        if (logPath != null && logPath.trim().length() > 0) {
            logBasePath = logPath;
        }
        File logPathDir = new File(logBasePath);
        if (!logPathDir.exists()) {
            logPathDir.mkdirs();
        }
        logBasePath = logPathDir.getPath();
    }

    public static String getLogPath() {
        return logBasePath;
    }

    public static String makeLogFileName(Date triggerDate, long logId) {
        File logFilePath = new File(getLogPath(), new SimpleDateFormat("yyyy-MM-dd").format(triggerDate));
        if (!logFilePath.exists()) {
            logFilePath.mkdirs();
        }
        return logFilePath.getPath().concat(File.separator).concat(String.valueOf(logId)).concat(".log");
    }

    public static void appendLog(String logFileName, String appendLog) {
        FileOutputStream fos;
        if (logFileName != null && logFileName.trim().length() != 0) {
            File logFile = new File(logFileName);
            if (!logFile.exists()) {
                try {
                    logFile.createNewFile();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                    return;
                }
            }
            if (appendLog == null) {
                appendLog = "";
            }
            try {
                String appendLog2 = appendLog + "\r\n";
                genRealTimeLog(logFile.getName(), appendLog2);
                fos = null;
                try {
                    fos = new FileOutputStream(logFile, true);
                    fos.write(appendLog2.getBytes("utf-8"));
                    fos.flush();
                    if (fos != null) {
                        try {
                            fos.close();
                        } catch (IOException e2) {
                            log.error(e2.getMessage(), e2);
                        }
                    }
                } catch (Exception e3) {
                    log.error(e3.getMessage(), e3);
                    if (fos != null) {
                        try {
                            fos.close();
                        } catch (IOException e4) {
                            log.error(e4.getMessage(), e4);
                        }
                    }
                }
            } catch (Throwable th) {
                if (fos != null) {
                    try {
                        fos.close();
                    } catch (IOException e5) {
                        log.error(e5.getMessage(), e5);
                    }
                }
                throw th;
            }
        }
    }

    private static void genRealTimeLog(String logFile, String appendLog) {
        StringRedisTemplate redisTemplate = (StringRedisTemplate) ApplicationContextHolder.context.getBean(StringRedisTemplate.class);
        if (redisTemplate == null) {
            log.error("系统没有配置RedisTemplate!不支持实时日志");
            return;
        }
        String logId = logFile.replaceAll(".log", "");
        redisTemplate.opsForList().rightPush("REALTIME_LOG:" + logId, appendLog);
        redisTemplate.expire("REALTIME_LOG:" + logId, 30, TimeUnit.MINUTES);
    }

    public static OfflineLogResult readLog(String logFileName, int fromLineNum) {
        LineNumberReader reader;
        if (logFileName == null || logFileName.trim().length() == 0) {
            return new OfflineLogResult(fromLineNum, 0, "readLog fail, logFile not found", true);
        }
        File logFile = new File(logFileName);
        if (!logFile.exists()) {
            return new OfflineLogResult(fromLineNum, 0, "readLog fail, logFile not exists", true);
        }
        try {
            StringBuffer logContentBuffer = new StringBuffer();
            int toLineNum = 0;
            reader = null;
            try {
                reader = new LineNumberReader(new InputStreamReader(new FileInputStream(logFile), "utf-8"));
                while (true) {
                    String line = reader.readLine();
                    if (line == null) {
                        break;
                    }
                    toLineNum = reader.getLineNumber();
                    if (toLineNum >= fromLineNum) {
                        logContentBuffer.append(line).append("\n");
                    }
                }
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        log.error(e.getMessage(), e);
                    }
                }
            } catch (IOException e2) {
                log.error(e2.getMessage(), e2);
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e3) {
                        log.error(e3.getMessage(), e3);
                    }
                }
            }
            return new OfflineLogResult(fromLineNum, toLineNum, logContentBuffer.toString(), false);
        } catch (Throwable th) {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e4) {
                    log.error(e4.getMessage(), e4);
                }
            }
            throw th;
        }
    }

    public static String readLines(File logFile) {
        BufferedReader reader = null;
        try {
            try {
                reader = new BufferedReader(new InputStreamReader(new FileInputStream(logFile), "utf-8"));
                if (reader != null) {
                    StringBuilder sb = new StringBuilder();
                    while (true) {
                        String line = reader.readLine();
                        if (line == null) {
                            break;
                        }
                        sb.append(line).append("\n");
                    }
                    String sb2 = sb.toString();
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (IOException e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                    return sb2;
                } else if (reader == null) {
                    return null;
                } else {
                    try {
                        reader.close();
                        return null;
                    } catch (IOException e2) {
                        log.error(e2.getMessage(), e2);
                        return null;
                    }
                }
            } catch (IOException e3) {
                log.error(e3.getMessage(), e3);
                if (reader == null) {
                    return null;
                }
                try {
                    reader.close();
                    return null;
                } catch (IOException e4) {
                    log.error(e4.getMessage(), e4);
                    return null;
                }
            }
        } catch (Throwable th) {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e5) {
                    log.error(e5.getMessage(), e5);
                }
            }
            throw th;
        }
    }
}
