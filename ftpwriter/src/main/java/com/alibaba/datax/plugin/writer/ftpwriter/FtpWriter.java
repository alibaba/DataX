package com.alibaba.datax.plugin.writer.ftpwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredStorageWriterUtil;
import com.alibaba.datax.plugin.writer.ftpwriter.util.Constant;
import com.alibaba.datax.plugin.writer.ftpwriter.util.IFtpHelper;
import com.alibaba.datax.plugin.writer.ftpwriter.util.SftpHelperImpl;
import com.alibaba.datax.plugin.writer.ftpwriter.util.StandardFtpHelperImpl;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

public class FtpWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;
        private Set<String> allFileExists = null;

        private String protocol;
        private String host;
        private int port;
        private String username;
        private String password;
        private int timeout;

        private IFtpHelper ftpHelper = null;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.validateParameter();
            UnstructuredStorageWriterUtil
                    .validateParameter(this.writerSliceConfig);
            try {
                RetryUtil.executeWithRetry(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        ftpHelper.loginFtpServer(host, username, password,
                                port, timeout);
                        return null;
                    }
                }, 3, 4000, true);
            } catch (Exception e) {
                String message = String
                        .format("与ftp服务器建立连接失败, host:%s, username:%s, port:%s, errorMessage:%s",
                                host, username, port, e.getMessage());
                LOG.error(message);
                throw DataXException.asDataXException(
                        FtpWriterErrorCode.FAIL_LOGIN, message, e);
            }
        }

        private void validateParameter() {
            this.writerSliceConfig
                    .getNecessaryValue(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME,
                            FtpWriterErrorCode.REQUIRED_VALUE);
            String path = this.writerSliceConfig.getNecessaryValue(Key.PATH,
                    FtpWriterErrorCode.REQUIRED_VALUE);
            if (!path.startsWith("/")) {
                String message = String.format("请检查参数path:%s,需要配置为绝对路径", path);
                LOG.error(message);
                throw DataXException.asDataXException(
                        FtpWriterErrorCode.ILLEGAL_VALUE, message);
            }

            this.host = this.writerSliceConfig.getNecessaryValue(Key.HOST,
                    FtpWriterErrorCode.REQUIRED_VALUE);
            this.username = this.writerSliceConfig.getNecessaryValue(
                    Key.USERNAME, FtpWriterErrorCode.REQUIRED_VALUE);
            this.password = this.writerSliceConfig.getNecessaryValue(
                    Key.PASSWORD, FtpWriterErrorCode.REQUIRED_VALUE);
            this.timeout = this.writerSliceConfig.getInt(Key.TIMEOUT,
                    Constant.DEFAULT_TIMEOUT);

            this.protocol = this.writerSliceConfig.getNecessaryValue(
                    Key.PROTOCOL, FtpWriterErrorCode.REQUIRED_VALUE);
            if ("sftp".equalsIgnoreCase(this.protocol)) {
                this.port = this.writerSliceConfig.getInt(Key.PORT,
                        Constant.DEFAULT_SFTP_PORT);
                this.ftpHelper = new SftpHelperImpl();
            } else if ("ftp".equalsIgnoreCase(this.protocol)) {
                this.port = this.writerSliceConfig.getInt(Key.PORT,
                        Constant.DEFAULT_FTP_PORT);
                this.ftpHelper = new StandardFtpHelperImpl();
            } else {
                throw DataXException.asDataXException(
                        FtpWriterErrorCode.ILLEGAL_VALUE, String.format(
                                "仅支持 ftp和sftp 传输协议 , 不支持您配置的传输协议: [%s]",
                                protocol));
            }
            this.writerSliceConfig.set(Key.PORT, this.port);
        }

        @Override
        public void prepare() {
            String path = this.writerSliceConfig.getString(Key.PATH);
            // warn: 这里用户需要配一个目录
            this.ftpHelper.mkDirRecursive(path);

            String fileName = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);
            String writeMode = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.WRITE_MODE);

            Set<String> allFileExists = this.ftpHelper.getAllFilesInDir(path,
                    fileName);
            this.allFileExists = allFileExists;

            // truncate option handler
            if ("truncate".equals(writeMode)) {
                LOG.info(String.format(
                        "由于您配置了writeMode truncate, 开始清理 [%s] 下面以 [%s] 开头的内容",
                        path, fileName));
                Set<String> fullFileNameToDelete = new HashSet<String>();
                for (String each : allFileExists) {
                    fullFileNameToDelete.add(UnstructuredStorageWriterUtil
                            .buildFilePath(path, each, null));
                }
                LOG.info(String.format(
                        "删除目录path:[%s] 下指定前缀fileName:[%s] 文件列表如下: [%s]", path,
                        fileName,
                        StringUtils.join(fullFileNameToDelete.iterator(), ", ")));

                this.ftpHelper.deleteFiles(fullFileNameToDelete);
            } else if ("append".equals(writeMode)) {
                LOG.info(String
                        .format("由于您配置了writeMode append, 写入前不做清理工作, [%s] 目录下写入相应文件名前缀  [%s] 的文件",
                                path, fileName));
                LOG.info(String.format(
                        "目录path:[%s] 下已经存在的指定前缀fileName:[%s] 文件列表如下: [%s]",
                        path, fileName,
                        StringUtils.join(allFileExists.iterator(), ", ")));
            } else if ("nonConflict".equals(writeMode)) {
                LOG.info(String.format(
                        "由于您配置了writeMode nonConflict, 开始检查 [%s] 下面的内容", path));
                if (!allFileExists.isEmpty()) {
                    LOG.info(String.format(
                            "目录path:[%s] 下指定前缀fileName:[%s] 冲突文件列表如下: [%s]",
                            path, fileName,
                            StringUtils.join(allFileExists.iterator(), ", ")));
                    throw DataXException
                            .asDataXException(
                                    FtpWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.",
                                            path));
                }
            } else {
                throw DataXException
                        .asDataXException(
                                FtpWriterErrorCode.ILLEGAL_VALUE,
                                String.format(
                                        "仅支持 truncate, append, nonConflict 三种模式, 不支持您配置的 writeMode 模式 : [%s]",
                                        writeMode));
            }
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {
            try {
                this.ftpHelper.logoutFtpServer();
            } catch (Exception e) {
                String message = String
                        .format("关闭与ftp服务器连接失败, host:%s, username:%s, port:%s, errorMessage:%s",
                                host, username, port, e.getMessage());
                LOG.error(message, e);
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return UnstructuredStorageWriterUtil.split(this.writerSliceConfig,
                    this.allFileExists, mandatoryNumber);
        }

    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        private String path;
        private String fileName;
        private String suffix;

        private String protocol;
        private String host;
        private int port;
        private String username;
        private String password;
        private int timeout;

        private IFtpHelper ftpHelper = null;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.path = this.writerSliceConfig.getString(Key.PATH);
            this.fileName = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);
            this.suffix = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.SUFFIX);

            this.host = this.writerSliceConfig.getString(Key.HOST);
            this.port = this.writerSliceConfig.getInt(Key.PORT);
            this.username = this.writerSliceConfig.getString(Key.USERNAME);
            this.password = this.writerSliceConfig.getString(Key.PASSWORD);
            this.timeout = this.writerSliceConfig.getInt(Key.TIMEOUT,
                    Constant.DEFAULT_TIMEOUT);
            this.protocol = this.writerSliceConfig.getString(Key.PROTOCOL);

            if ("sftp".equalsIgnoreCase(this.protocol)) {
                this.ftpHelper = new SftpHelperImpl();
            } else if ("ftp".equalsIgnoreCase(this.protocol)) {
                this.ftpHelper = new StandardFtpHelperImpl();
            }
            try {
                RetryUtil.executeWithRetry(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        ftpHelper.loginFtpServer(host, username, password,
                                port, timeout);
                        return null;
                    }
                }, 3, 4000, true);
            } catch (Exception e) {
                String message = String
                        .format("与ftp服务器建立连接失败, host:%s, username:%s, port:%s, errorMessage:%s",
                                host, username, port, e.getMessage());
                LOG.error(message);
                throw DataXException.asDataXException(
                        FtpWriterErrorCode.FAIL_LOGIN, message, e);
            }
        }

        @Override
        public void prepare() {

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            LOG.info("begin do write...");
            String fileFullPath = UnstructuredStorageWriterUtil.buildFilePath(
                    this.path, this.fileName, this.suffix);
            LOG.info(String.format("write to file : [%s]", fileFullPath));

            OutputStream outputStream = null;
            try {
                outputStream = this.ftpHelper.getOutputStream(fileFullPath);
                UnstructuredStorageWriterUtil.writeToStream(lineReceiver,
                        outputStream, this.writerSliceConfig, this.fileName,
                        this.getTaskPluginCollector());
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        FtpWriterErrorCode.WRITE_FILE_IO_ERROR,
                        String.format("无法创建待写文件 : [%s]", this.fileName), e);
            } finally {
                IOUtils.closeQuietly(outputStream);
            }
            LOG.info("end do write");
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {
            try {
                this.ftpHelper.logoutFtpServer();
            } catch (Exception e) {
                String message = String
                        .format("关闭与ftp服务器连接失败, host:%s, username:%s, port:%s, errorMessage:%s",
                                host, username, port, e.getMessage());
                LOG.error(message, e);
            }
        }
    }
}
