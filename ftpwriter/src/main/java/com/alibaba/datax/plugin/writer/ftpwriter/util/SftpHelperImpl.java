package com.alibaba.datax.plugin.writer.ftpwriter.util;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.writer.ftpwriter.FtpWriterErrorCode;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.ChannelSftp.LsEntry;

public class SftpHelperImpl implements IFtpHelper {
    private static final Logger LOG = LoggerFactory
            .getLogger(SftpHelperImpl.class);

    private Session session = null;
    private ChannelSftp channelSftp = null;

    @Override
    public void loginFtpServer(String host, String username, String password,
            int port, int timeout) {
        JSch jsch = new JSch();
        try {
            this.session = jsch.getSession(username, host, port);
            if (this.session == null) {
                throw DataXException
                        .asDataXException(FtpWriterErrorCode.FAIL_LOGIN,
                                "创建ftp连接this.session失败,无法通过sftp与服务器建立链接，请检查主机名和用户名是否正确.");
            }

            this.session.setPassword(password);
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            // config.put("PreferredAuthentications", "password");
            this.session.setConfig(config);
            this.session.setTimeout(timeout);
            this.session.connect();

            this.channelSftp = (ChannelSftp) this.session.openChannel("sftp");
            this.channelSftp.connect();
        } catch (JSchException e) {
            if (null != e.getCause()) {
                String cause = e.getCause().toString();
                String unknownHostException = "java.net.UnknownHostException: "
                        + host;
                String illegalArgumentException = "java.lang.IllegalArgumentException: port out of range:"
                        + port;
                String wrongPort = "java.net.ConnectException: Connection refused";
                if (unknownHostException.equals(cause)) {
                    String message = String
                            .format("请确认ftp服务器地址是否正确，无法连接到地址为: [%s] 的ftp服务器, errorMessage:%s",
                                    host, e.getMessage());
                    LOG.error(message);
                    throw DataXException.asDataXException(
                            FtpWriterErrorCode.FAIL_LOGIN, message, e);
                } else if (illegalArgumentException.equals(cause)
                        || wrongPort.equals(cause)) {
                    String message = String.format(
                            "请确认连接ftp服务器端口是否正确，错误的端口: [%s], errorMessage:%s",
                            port, e.getMessage());
                    LOG.error(message);
                    throw DataXException.asDataXException(
                            FtpWriterErrorCode.FAIL_LOGIN, message, e);
                }
            } else {
                String message = String
                        .format("与ftp服务器建立连接失败,请检查主机、用户名、密码是否正确, host:%s, port:%s, username:%s, errorMessage:%s",
                                host, port, username, e.getMessage());
                LOG.error(message);
                throw DataXException.asDataXException(
                        FtpWriterErrorCode.FAIL_LOGIN, message);
            }
        }

    }

    @Override
    public void logoutFtpServer() {
        if (this.channelSftp != null) {
            this.channelSftp.disconnect();
            this.channelSftp = null;
        }
        if (this.session != null) {
            this.session.disconnect();
            this.session = null;
        }
    }

    @Override
    public void mkdir(String directoryPath) {
        boolean isDirExist = false;
        try {
            this.printWorkingDirectory();
            SftpATTRS sftpATTRS = this.channelSftp.lstat(directoryPath);
            isDirExist = sftpATTRS.isDir();
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().equals("no such file")) {
                LOG.warn(String.format(
                        "您的配置项path:[%s]不存在，将尝试进行目录创建, errorMessage:%s",
                        directoryPath, e.getMessage()), e);
                isDirExist = false;
            }
        }
        if (!isDirExist) {
            try {
                // warn 检查mkdir -p
                this.channelSftp.mkdir(directoryPath);
            } catch (SftpException e) {
                String message = String
                        .format("创建目录:%s时发生I/O异常,请确认与ftp服务器的连接正常,拥有目录创建权限, errorMessage:%s",
                                directoryPath, e.getMessage());
                LOG.error(message, e);
                throw DataXException
                        .asDataXException(
                                FtpWriterErrorCode.COMMAND_FTP_IO_EXCEPTION,
                                message, e);
            }
        }
    }

    @Override
    public void mkDirRecursive(String directoryPath){
        boolean isDirExist = false;
        try {
            this.printWorkingDirectory();
            SftpATTRS sftpATTRS = this.channelSftp.lstat(directoryPath);
            isDirExist = sftpATTRS.isDir();
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().equals("no such file")) {
                LOG.warn(String.format(
                        "您的配置项path:[%s]不存在，将尝试进行目录创建, errorMessage:%s",
                        directoryPath, e.getMessage()), e);
                isDirExist = false;
            }
        }
        if (!isDirExist) {
            StringBuilder dirPath = new StringBuilder();
            dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
            String[] dirSplit = StringUtils.split(directoryPath,IOUtils.DIR_SEPARATOR_UNIX);
            try {
                // ftp server不支持递归创建目录,只能一级一级创建
                for(String dirName : dirSplit){
                    dirPath.append(dirName);
                    mkDirSingleHierarchy(dirPath.toString());
                    dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
                }
            } catch (SftpException e) {
                String message = String
                        .format("创建目录:%s时发生I/O异常,请确认与ftp服务器的连接正常,拥有目录创建权限, errorMessage:%s",
                                directoryPath, e.getMessage());
                LOG.error(message, e);
                throw DataXException
                        .asDataXException(
                                FtpWriterErrorCode.COMMAND_FTP_IO_EXCEPTION,
                                message, e);
            }
        }
    }

    public boolean mkDirSingleHierarchy(String directoryPath) throws SftpException {
        boolean isDirExist = false;
        try {
            SftpATTRS sftpATTRS = this.channelSftp.lstat(directoryPath);
            isDirExist = sftpATTRS.isDir();
        } catch (SftpException e) {
            if(!isDirExist){
                LOG.info(String.format("正在逐级创建目录 [%s]",directoryPath));
                this.channelSftp.mkdir(directoryPath);
                return true;
            }
        }
        if(!isDirExist){
            LOG.info(String.format("正在逐级创建目录 [%s]",directoryPath));
            this.channelSftp.mkdir(directoryPath);
        }
        return true;
    }

    @Override
    public OutputStream getOutputStream(String filePath) {
        try {
            this.printWorkingDirectory();
            String parentDir = filePath.substring(0,
                    StringUtils.lastIndexOf(filePath, IOUtils.DIR_SEPARATOR));
            this.channelSftp.cd(parentDir);
            this.printWorkingDirectory();
            OutputStream writeOutputStream = this.channelSftp.put(filePath,
                    ChannelSftp.APPEND);
            String message = String.format(
                    "打开FTP文件[%s]获取写出流时出错,请确认文件%s有权限创建，有权限写出等", filePath,
                    filePath);
            if (null == writeOutputStream) {
                throw DataXException.asDataXException(
                        FtpWriterErrorCode.OPEN_FILE_ERROR, message);
            }
            return writeOutputStream;
        } catch (SftpException e) {
            String message = String.format(
                    "写出文件[%s] 时出错,请确认文件%s有权限写出, errorMessage:%s", filePath,
                    filePath, e.getMessage());
            LOG.error(message);
            throw DataXException.asDataXException(
                    FtpWriterErrorCode.OPEN_FILE_ERROR, message);
        }
    }

    @Override
    public String getRemoteFileContent(String filePath) {
        try {
            this.completePendingCommand();
            this.printWorkingDirectory();
            String parentDir = filePath.substring(0,
                    StringUtils.lastIndexOf(filePath, IOUtils.DIR_SEPARATOR));
            this.channelSftp.cd(parentDir);
            this.printWorkingDirectory();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(22);
            this.channelSftp.get(filePath, outputStream);
            String result = outputStream.toString();
            IOUtils.closeQuietly(outputStream);
            return result;
        } catch (SftpException e) {
            String message = String.format(
                    "写出文件[%s] 时出错,请确认文件%s有权限写出, errorMessage:%s", filePath,
                    filePath, e.getMessage());
            LOG.error(message);
            throw DataXException.asDataXException(
                    FtpWriterErrorCode.OPEN_FILE_ERROR, message);
        }
    }

    @Override
    public Set<String> getAllFilesInDir(String dir, String prefixFileName) {
        Set<String> allFilesWithPointedPrefix = new HashSet<String>();
        try {
            this.printWorkingDirectory();
            @SuppressWarnings("rawtypes")
            Vector allFiles = this.channelSftp.ls(dir);
            LOG.debug(String.format("ls: %s", JSON.toJSONString(allFiles,
                    JSONWriter.Feature.UseSingleQuotes)));
            for (int i = 0; i < allFiles.size(); i++) {
                LsEntry le = (LsEntry) allFiles.get(i);
                String strName = le.getFilename();
                if (strName.startsWith(prefixFileName)) {
                    allFilesWithPointedPrefix.add(strName);
                }
            }
        } catch (SftpException e) {
            String message = String
                    .format("获取path:[%s] 下文件列表时发生I/O异常,请确认与ftp服务器的连接正常,拥有目录ls权限, errorMessage:%s",
                            dir, e.getMessage());
            LOG.error(message);
            throw DataXException.asDataXException(
                    FtpWriterErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
        }
        return allFilesWithPointedPrefix;
    }

    @Override
    public void deleteFiles(Set<String> filesToDelete) {
        String eachFile = null;
        try {
            this.printWorkingDirectory();
            for (String each : filesToDelete) {
                LOG.info(String.format("delete file [%s].", each));
                eachFile = each;
                this.channelSftp.rm(each);
            }
        } catch (SftpException e) {
            String message = String.format(
                    "删除文件:[%s] 时发生异常,请确认指定文件有删除权限,以及网络交互正常, errorMessage:%s",
                    eachFile, e.getMessage());
            LOG.error(message);
            throw DataXException.asDataXException(
                    FtpWriterErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
        }
    }

    private void printWorkingDirectory() {
        try {
            LOG.info(String.format("current working directory:%s",
                    this.channelSftp.pwd()));
        } catch (Exception e) {
            LOG.warn(String.format("printWorkingDirectory error:%s",
                    e.getMessage()));
        }
    }

    @Override
    public void completePendingCommand() {
    }

}
