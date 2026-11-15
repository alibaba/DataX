package com.leehom.arch.datax.plugin.rdb2graph.common;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @类名: ResourceLoaderUtil
 * @说明: 资源加载工具类 
 *
 * @author   leehom
 * @Date	 2018-9-9 上午12:05:39
 * 修改记录：
 *
 * @see 	 
 */
public class ResourceLoaderUtil {
	
	private static Logger log = LoggerFactory.getLogger(ResourceLoaderUtil.class);

	/**
	 * 
	 * get a class with the name specified
	 * 
	 * @paramclassName
	 * 
	 * @return
	 * 
	 */
	public static Class loadClass(String className) {
		try {
			return getClassLoader().loadClass(className);
		}
		catch (ClassNotFoundException e) {
			throw new RuntimeException("class not found '" + className + "'", e);
		}
	}

	/**
	 * 
	 * get the class loader
	 * 
	 * @return
	 * 
	 */
	public static ClassLoader getClassLoader() {
		return ResourceLoaderUtil.class.getClassLoader();
	}

	/**
	 * 
	 * get the file that from the path that related to current class path
	 * example, there is the file root like this:
	 *   |--resource/xml/target.xml
	 *   |--classes
	 * in order to get the xml file the api should called like this:
	 * getStream("../resource/xml/target.xml");
	 * @paramrelativePath
	 * 
	 * @return 
	 * 
	 * @throwsIOException
	 * 
	 * @throwsMalformedURLException
	 * 
	 */
	public static InputStream getResourceStream(String relativePath) throws MalformedURLException, IOException {
		if (!relativePath.contains("../")) {
			return getClassLoader().getResourceAsStream(relativePath);
		}
		else {
			return ResourceLoaderUtil.getStreamByExtendResource(relativePath);
		}
	}

	/**
	 * 
	 * 
	 * 
	 * @paramurl
	 * 
	 * @return
	 * 
	 * @throwsIOException
	 * 
	 */
	public static InputStream getStream(URL url) throws IOException {
		if (url != null) {
			return url.openStream();
		}
		else {
			return null;
		}
	}

	/**
	 * 
	 * get the file that from the path that related to current class path
	 * example, there is the file root like this:
	 *   |--resource/xml/target.xml
	 *   |--classes
	 * in order to get the xml file the api should called like this:
	 * getStream("../resource/xml/target.xml");
	 * @return
	 * 
	 * @throwsMalformedURLException
	 * 
	 * @throwsIOException
	 * 
	 */
	public static InputStream getStreamByExtendResource(String relativePath) throws MalformedURLException, IOException {
		return ResourceLoaderUtil.getStream(ResourceLoaderUtil.getExtendResource(relativePath));
	}

	/**
	 * 
	 * 
	 * @paramresource
	 * 
	 * @return
	 * 
	 */
	public static Properties getProperties(String resource) {
		Properties properties = new Properties();
		try {
			properties.load(getResourceStream(resource));
		}
		catch (IOException e) {
			throw new RuntimeException("couldn't load properties file '" + resource + "'", e);
		}
		return properties;
	}

	/**
	 * 
	 * get the absolute path of the class loader of this Class
	 * 
	 * 
	 * @return
	 * 
	 */
	public static String getAbsolutePathOfClassLoaderClassPath() {
		//ClassLoaderUtil.log.info(ClassLoaderUtil.getClassLoader().getResource("").toString());
		return ResourceLoaderUtil.getClassLoader().getResource("").getPath();
	}

	/**
	 * 
	 * get the file that from the path that related to current class path
	 * example, there is the file root like this:
	 *   |--resource/xml/target.xml
	 *   |--classes
	 * in order to get the xml file the api should called like this:
	 * getStream("../resource/xml/target.xml");
	 * 
	 * @throwsMalformedURLException
	 * 
	 */
	public static URL getExtendResource(String relativePath) throws MalformedURLException {
		//ClassLoaderUtil.log.info("The income relative path:" + relativePath);
		// ClassLoaderUtil.log.info(Integer.valueOf(relativePath.indexOf("../"))) ;
		if (!relativePath.contains("../")) {
			return ResourceLoaderUtil.getResource(relativePath);
		}
		String classPathAbsolutePath = ResourceLoaderUtil.getAbsolutePathOfClassLoaderClassPath();
		if (relativePath.substring(0, 1).equals("/")) {
			relativePath = relativePath.substring(1);
		}
		//ClassLoaderUtil.log.info(Integer.valueOf(relativePath.lastIndexOf("../")));
		String wildcardString = relativePath.substring(0, relativePath.lastIndexOf("../") + 3);
		relativePath = relativePath.substring(relativePath.lastIndexOf("../") + 3);
		int containSum = ResourceLoaderUtil.containSum(wildcardString, "../");
		classPathAbsolutePath = ResourceLoaderUtil.cutLastString(classPathAbsolutePath, "/", containSum);
		String resourceAbsolutePath = classPathAbsolutePath + relativePath;
		//ClassLoaderUtil.log.info("The income absolute path:" + resourceAbsolutePath);
		URL resourceAbsoluteURL = new URL(resourceAbsolutePath);
		return resourceAbsoluteURL;
	}

	/**
	 * 
	 * 
	 * 
	 * @paramsource
	 * 
	 * @paramdest
	 * 
	 * @return
	 * 
	 */
	private static int containSum(String source, String dest) {
		int containSum = 0;
		int destLength = dest.length();
		while (source.contains(dest)) {
			containSum = containSum + 1;
			source = source.substring(destLength);
		}
		return containSum;
	}

	/**
	 * 
	 * 
	 * 
	 * @paramsource
	 * 
	 * @paramdest
	 * 
	 * @paramnum
	 * 
	 * @return
	 * 
	 */
	private static String cutLastString(String source, String dest, int num) {
		// String cutSource=null;
		for (int i = 0; i < num; i++) {
			source = source.substring(0, source.lastIndexOf(dest, source.length() - 2) + 1);
		}
		return source;
	}

	/**
	 * 
	 * 
	 * 
	 * @paramresource
	 * 
	 * @return
	 * 
	 */
	public static URL getResource(String resource) {
		//ClassLoaderUtil.log.info("The income classpath related path:" + resource);
		return ResourceLoaderUtil.getClassLoader().getResource(resource);
	}

	public static File getFile(String resource) throws URISyntaxException {
		URL url = ResourceLoaderUtil.getClassLoader().getResource(resource);
		if(url==null)
			return null;
		File file=new File(url.getPath());
		return file;
	}
	
}
