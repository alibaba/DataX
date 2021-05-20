package com.alibaba.datax.plugin.writer.oceanbasev10writer.task;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriterThreadPool {
    private static final Logger LOG = LoggerFactory.getLogger(InsertTask.class);
	
	private static ExecutorService executorService = Executors.newCachedThreadPool();
	
	public WriterThreadPool() {
	}
	
	public static ExecutorService getInstance() {
		return executorService;
	}
	
	public static synchronized void shutdown() {
		LOG.info("start shutdown executor service...");
		executorService.shutdown();
		LOG.info("shutdown executor service success...");
	}
	
	public static synchronized void execute(InsertTask task) {
		executorService.execute(task);
	}
	
	public static synchronized void executeBatch(List<InsertTask> tasks) {
		for (InsertTask task : tasks) {
			executorService.execute(task);
		}
	}
}
