/**
 * %datax-graph%
 * %v1.0%
 */
package com.leehom.arch.datax.rdb2graph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.core.Engine;

/**
 * @类名: EngineMain
 * @说明: datax engine主入口，执行两个engine
 *        1. 行数据同步
 *        2. 关系同步
 *
 * @author   leehom
 * @Date	 2022年4月26日 下午6:56:23
 * 修改记录：
 *
 * @see 	 
 */
public class EngineMain {
    
	public static final Logger log = LoggerFactory.getLogger(EngineMain.class);
	
    public static void main(String[] args) {
        System.setProperty("datax.home", getCurrentClasspath());
        try {
        	log.info("行数据同步开始>>>");
        	String[] dataxArgs1 = {"-job", getCurrentClasspath() + "/job/mysql/row2node.json", "-mode", "standalone", "-jobid", "1001"};
            // Engine.entry(dataxArgs1);
            log.info("行数据同步完成.");
        } catch (Throwable e) {
        	log.error(e.getMessage());
        }
        log.info("关系同步开始>>>");
        try {
        	String[] dataxArgs2 = {"-job", getCurrentClasspath() + "/job/mysql/fk2rel.json", "-mode", "standalone", "-jobid", "1001"};
			Engine.entry(dataxArgs2);
			log.info("关系同步完成.");
		} catch (Throwable e) {
			log.error(e.getMessage());
		}
    }
    
    // 
    public static String getCurrentClasspath() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        String currentClasspath = classLoader.getResource("").getPath();
        // 当前操作系统
        String osName = System.getProperty("os.name");
        if (osName.startsWith("Windows")) {
            // 删除path中最前面的/
            currentClasspath = currentClasspath.substring(1);
        }
        return currentClasspath;
    }
}
