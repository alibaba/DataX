package com.alibaba.datax.plugin.reader.ftpreader;

import java.io.InputStream;
import java.util.HashSet;
import java.util.List;

public abstract class FtpHelper {
	/**
	 * 
	* @Title: LoginFtpServer 
	* @Description: 与ftp服务器建立连接
	* @param @param host
	* @param @param username
	* @param @param password
	* @param @param port
	* @param @param timeout
	* @param @param connectMode     
	* @return void 
	* @throws
	 */
	public abstract void loginFtpServer(String host, String username, String password, int port, int timeout,String connectMode) ;
	/**
	 * 
	* @Title: LogoutFtpServer 
	* todo 方法名首字母
	* @Description: 断开与ftp服务器的连接 
	* @param      
	* @return void 
	* @throws
	 */
	public abstract void logoutFtpServer();
	/**
	 * 
	* @Title: isDirExist 
	* @Description: 判断指定路径是否是目录
	* @param @param directoryPath
	* @param @return     
	* @return boolean 
	* @throws
	 */
	public abstract boolean isDirExist(String directoryPath);
	/**
	 * 
	* @Title: isFileExist 
	* @Description: 判断指定路径是否是文件
	* @param @param filePath
	* @param @return     
	* @return boolean 
	* @throws
	 */
	public abstract boolean isFileExist(String filePath);
	/**
	 * 
	* @Title: isSymbolicLink 
	* @Description: 判断指定路径是否是软链接
	* @param @param filePath
	* @param @return     
	* @return boolean 
	* @throws
	 */
	public abstract boolean isSymbolicLink(String filePath);
	/**
	 * 
	* @Title: getListFiles 
	* @Description: 递归获取指定路径下符合条件的所有文件绝对路径
	* @param @param directoryPath
	* @param @param parentLevel 父目录的递归层数（首次为0）
	* @param @param maxTraversalLevel 允许的最大递归层数
	* @param @return     
	* @return HashSet<String> 
	* @throws
	 */
	public abstract HashSet<String> getListFiles(String directoryPath, int parentLevel, int maxTraversalLevel);
	
	/**
	 * 
	* @Title: getInputStream 
	* @Description: 获取指定路径的输入流
	* @param @param filePath
	* @param @return     
	* @return InputStream 
	* @throws
	 */
	public abstract InputStream getInputStream(String filePath);
	
	/**
	 * 
	* @Title: getAllFiles 
	* @Description: 获取指定路径列表下符合条件的所有文件的绝对路径  
	* @param @param srcPaths 路径列表
	* @param @param parentLevel 父目录的递归层数（首次为0）
	* @param @param maxTraversalLevel 允许的最大递归层数
	* @param @return     
	* @return HashSet<String> 
	* @throws
	 */
	public HashSet<String> getAllFiles(List<String> srcPaths, int parentLevel, int maxTraversalLevel){
		HashSet<String> sourceAllFiles = new HashSet<String>();
		if (!srcPaths.isEmpty()) {
			for (String eachPath : srcPaths) {
				sourceAllFiles.addAll(getListFiles(eachPath, parentLevel, maxTraversalLevel));
			}
		}
		return sourceAllFiles;
	}

}
