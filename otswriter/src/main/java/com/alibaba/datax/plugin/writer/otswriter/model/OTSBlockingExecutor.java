package com.alibaba.datax.plugin.writer.otswriter.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * 单个Channel会多线程并发的写入数据到OTS中，需要使用一个固定的线程池来执行Runnable对象，同时当
 * 线程池满时，阻塞execute方法。原生的Executor并不能做到阻塞execute方法。只是当queue满时，
 * 方法抛出默认RejectedExecutionException，或者我们实现RejectedExecutionHandler，
 * 这两种方法都无法满足阻塞用户请求的需求，所以我们用信号量来实现了一个阻塞的Executor
 * @author redchen
 *
 */
public class OTSBlockingExecutor {
    private final ExecutorService exec;
    private final Semaphore semaphore;
    
    private static final Logger LOG = LoggerFactory.getLogger(OTSBlockingExecutor.class);

    public OTSBlockingExecutor(int concurrency) {
        this.exec = new ThreadPoolExecutor(
                concurrency, concurrency,  
                0L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        this.semaphore = new Semaphore(concurrency);
    }

    public void execute(final Runnable task)
            throws InterruptedException {
        LOG.debug("Begin execute");
        try {
            semaphore.acquire();
            exec.execute(new Runnable() {
                public void run() {
                    try {
                        task.run();
                    } finally {
                        semaphore.release();
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            semaphore.release();
            throw new RuntimeException(OTSErrorMessage.INSERT_TASK_ERROR);
        }
        LOG.debug("End execute");
    }
    
    public void shutdown() throws InterruptedException {
        this.exec.shutdown();
        while (!this.exec.awaitTermination(1, TimeUnit.SECONDS)){} 
    }
}