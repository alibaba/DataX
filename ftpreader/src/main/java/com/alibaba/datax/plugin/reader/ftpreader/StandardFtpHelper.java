package com.alibaba.datax.plugin.reader.ftpreader;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.HashSet;

import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;

public class StandardFtpHelper extends FtpHelper {
	private static final Logger LOG = LoggerFactory.getLogger(StandardFtpHelper.class);
	FTPClient ftpClient = null;

	@Override
	public void loginFtpServer(String host, String username, String password, int port, int timeout,
			String connectMode) {
		ftpClient = new FTPClient();
		try {
			// 连接
			ftpClient.connect(host, port);
			// 登录
			ftpClient.login(username, password);
			// 不需要写死ftp server的OS TYPE,FTPClient getSystemType()方法会自动识别
			// ftpClient.configure(new FTPClientConfig(FTPClientConfig.SYST_UNIX));
			ftpClient.setConnectTimeout(timeout);
			ftpClient.setDataTimeout(timeout);
			if ("PASV".equals(connectMode)) {
				ftpClient.enterRemotePassiveMode();
				ftpClient.enterLocalPassiveMode();
			} else if ("PORT".equals(connectMode)) {
				ftpClient.enterLocalActiveMode();
				// ftpClient.enterRemoteActiveMode(host, port);
			}
			int reply = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				ftpClient.disconnect();
				String message = String.format("与ftp服务器建立连接失败,请检查用户名和密码是否正确: [%s]",
						"message:host =" + host + ",username = " + username + ",port =" + port);
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message);
			}
			//设置命令传输编码
			String fileEncoding = System.getProperty("file.encoding");
			ftpClient.setControlEncoding(fileEncoding);
		} catch (UnknownHostException e) {
			String message = String.format("请确认ftp服务器地址是否正确，无法连接到地址为: [%s] 的ftp服务器", host);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message, e);
		} catch (IllegalArgumentException e) {
			String message = String.format("请确认连接ftp服务器端口是否正确，错误的端口: [%s] ", port);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message, e);
		} catch (Exception e) {
			String message = String.format("与ftp服务器建立连接失败 : [%s]",
					"message:host =" + host + ",username = " + username + ",port =" + port);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message, e);
		}

	}

	@Override
	public void logoutFtpServer() {
		if (ftpClient.isConnected()) {
			try {
				//todo ftpClient.completePendingCommand();//打开流操作之后必须，原因还需要深究
				ftpClient.logout();
			} catch (IOException e) {
				String message = "与ftp服务器断开连接失败";
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_DISCONNECT, message, e);
			}finally {
				if(ftpClient.isConnected()){
					try {
						ftpClient.disconnect();
					} catch (IOException e) {
						String message = "与ftp服务器断开连接失败";
						LOG.error(message);
						throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_DISCONNECT, message, e);
					}
				}

			}
		}
	}

	@Override
	public boolean isDirExist(String directoryPath) {
		try {
			return ftpClient.changeWorkingDirectory(new String(directoryPath.getBytes(),FTP.DEFAULT_CONTROL_ENCODING));
		} catch (IOException e) {
			String message = String.format("进入目录：[%s]时发生I/O异常,请确认与ftp服务器的连接正常", directoryPath);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
		}
	}

	@Override
	public boolean isFileExist(String filePath) {
		boolean isExitFlag = false;
		try {
			FTPFile[] ftpFiles = ftpClient.listFiles(new String(filePath.getBytes(),FTP.DEFAULT_CONTROL_ENCODING));
			if (ftpFiles.length == 1 && ftpFiles[0].isFile()) {
				isExitFlag = true;
			}
		} catch (IOException e) {
			String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", filePath);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
		}
		return isExitFlag;
	}

	@Override
	public boolean isSymbolicLink(String filePath) {
		boolean isExitFlag = false;
		try {
			FTPFile[] ftpFiles = ftpClient.listFiles(new String(filePath.getBytes(),FTP.DEFAULT_CONTROL_ENCODING));
			if (ftpFiles.length == 1 && ftpFiles[0].isSymbolicLink()) {
				isExitFlag = true;
			}
		} catch (IOException e) {
			String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", filePath);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
		}
		return isExitFlag;
	}

	HashSet<String> sourceFiles = new HashSet<String>();
	@Override
	public HashSet<String> getListFiles(String directoryPath, int parentLevel, int maxTraversalLevel) {
		if(parentLevel < maxTraversalLevel){
			String parentPath = null;// 父级目录,以'/'结尾
			int pathLen = directoryPath.length();
			if (directoryPath.contains("*") || directoryPath.contains("?")) {
				// path是正则表达式				
				String subPath  = UnstructuredStorageReaderUtil.getRegexPathParentPath(directoryPath);
				if (isDirExist(subPath)) {
					parentPath = subPath;
				} else {
					String message = String.format("不能进入目录：[%s]," + "请确认您的配置项path:[%s]存在，且配置的用户有权限进入", subPath,
							directoryPath);
					LOG.error(message);
					throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
				}
			} else if (isDirExist(directoryPath)) {
				// path是目录
				if (directoryPath.charAt(pathLen - 1) == IOUtils.DIR_SEPARATOR) {
					parentPath = directoryPath;
				} else {
					parentPath = directoryPath + IOUtils.DIR_SEPARATOR;
				}
			} else if (isFileExist(directoryPath)) {
				// path指向具体文件
				sourceFiles.add(directoryPath);
				return sourceFiles;
			} else if(isSymbolicLink(directoryPath)){
				//path是链接文件
				String message = String.format("文件:[%s]是链接文件，当前不支持链接文件的读取", directoryPath);
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.LINK_FILE, message);
			}else {
				String message = String.format("请确认您的配置项path:[%s]存在，且配置的用户有权限读取", directoryPath);
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
			}

			try {
				FTPFile[] fs = ftpClient.listFiles(new String(directoryPath.getBytes(),FTP.DEFAULT_CONTROL_ENCODING));
				for (FTPFile ff : fs) {
					String strName = ff.getName();
					String filePath = parentPath + strName;
					if (ff.isDirectory()) {
						if (!(strName.equals(".") || strName.equals(".."))) {
							//递归处理
							getListFiles(filePath, parentLevel+1, maxTraversalLevel);
						}
					} else if (ff.isFile()) {
						// 是文件
						sourceFiles.add(filePath);						
					} else if(ff.isSymbolicLink()){
						//是链接文件
						String message = String.format("文件:[%s]是链接文件，当前不支持链接文件的读取", filePath);
						LOG.error(message);
						throw DataXException.asDataXException(FtpReaderErrorCode.LINK_FILE, message);
					}else {
						String message = String.format("请确认path:[%s]存在，且配置的用户有权限读取", filePath);
						LOG.error(message);
						throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
					}
				} // end for FTPFile
			} catch (IOException e) {
				String message = String.format("获取path：[%s] 下文件列表时发生I/O异常,请确认与ftp服务器的连接正常", directoryPath);
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
			}
			return sourceFiles;
			
		} else{
			//超出最大递归层数
			String message = String.format("获取path：[%s] 下文件列表时超出最大层数,请确认路径[%s]下不存在软连接文件", directoryPath, directoryPath);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.OUT_MAX_DIRECTORY_LEVEL, message);
		}
	}

	@Override
	public InputStream getInputStream(String filePath) {
		try {
			return ftpClient.retrieveFileStream(new String(filePath.getBytes(),FTP.DEFAULT_CONTROL_ENCODING));
		} catch (IOException e) {
			String message = String.format("读取文件 : [%s] 时出错,请确认文件：[%s]存在且配置的用户有权限读取", filePath, filePath);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.OPEN_FILE_ERROR, message);
		}
	}

}
