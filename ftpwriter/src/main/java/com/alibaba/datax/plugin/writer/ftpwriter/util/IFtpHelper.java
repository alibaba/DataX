package com.alibaba.datax.plugin.writer.ftpwriter.util;

import java.io.OutputStream;
import java.util.Set;

public interface IFtpHelper {

    //使用被动方式
    public void loginFtpServer(String host, String username, String password, int port, int timeout);

    public void logoutFtpServer();

    /**
     * warn: 不支持递归创建, 比如 mkdir -p
     * */
    public void mkdir(String directoryPath);

    /**
     * 支持目录递归创建
     */
    public void mkDirRecursive(String directoryPath);

    public OutputStream getOutputStream(String filePath);
    
    public String getRemoteFileContent(String filePath);

    public Set<String> getAllFilesInDir(String dir, String prefixFileName);

    /**
     * warn: 不支持文件夹删除, 比如 rm -rf
     * */
    public void deleteFiles(Set<String> filesToDelete);
    
    public void completePendingCommand();

}
