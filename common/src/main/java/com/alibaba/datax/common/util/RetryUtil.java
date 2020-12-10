package com.alibaba.datax.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

public final class RetryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(RetryUtil.class);

    private static final long MAX_SLEEP_MILLISECOND = 256 * 1000;

    /**
     * 重试次数工具方法.
     *
     * @param callable               实际逻辑
     * @param retryTimes             最大重试次数（>1）
     * @param sleepTimeInMilliSecond 运行失败后休眠对应时间再重试
     * @param exponential            休眠时间是否指数递增
     * @param <T>                    返回值类型
     * @return 经过重试的callable的执行结果
     */
    public static <T> T executeWithRetry(Callable<T> callable,
                                         int retryTimes,
                                         long sleepTimeInMilliSecond,
                                         boolean exponential) throws Exception {
        Retry retry = new Retry();
        return retry.doRetry(callable, retryTimes, sleepTimeInMilliSecond, exponential, null);
    }
    
    /**
     * 重试次数工具方法.
     *
     * @param callable               实际逻辑
     * @param retryTimes             最大重试次数（>1）
     * @param sleepTimeInMilliSecond 运行失败后休眠对应时间再重试
     * @param exponential            休眠时间是否指数递增
     * @param <T>                    返回值类型
     * @param retryExceptionClasss   出现指定的异常类型时才进行重试
     * @return 经过重试的callable的执行结果
     */
    public static <T> T executeWithRetry(Callable<T> callable,
                                         int retryTimes,
                                         long sleepTimeInMilliSecond,
                                         boolean exponential,
                                         List<Class<?>> retryExceptionClasss) throws Exception {
        Retry retry = new Retry();
        return retry.doRetry(callable, retryTimes, sleepTimeInMilliSecond, exponential, retryExceptionClasss);
    }

    /**
     * 在外部线程执行并且重试。每次执行需要在timeoutMs内执行完，不然视为失败。
     * 执行异步操作的线程池从外部传入，线程池的共享粒度由外部控制。比如，HttpClientUtil共享一个线程池。
     * <p/>
     * 限制条件：仅仅能够在阻塞的时候interrupt线程
     *
     * @param callable               实际逻辑
     * @param retryTimes             最大重试次数（>1）
     * @param sleepTimeInMilliSecond 运行失败后休眠对应时间再重试
     * @param exponential            休眠时间是否指数递增
     * @param timeoutMs              callable执行超时时间，毫秒
     * @param executor               执行异步操作的线程池
     * @param <T>                    返回值类型
     * @return 经过重试的callable的执行结果
     */
    public static <T> T asyncExecuteWithRetry(Callable<T> callable,
                                              int retryTimes,
                                              long sleepTimeInMilliSecond,
                                              boolean exponential,
                                              long timeoutMs,
                                              ThreadPoolExecutor executor) throws Exception {
        Retry retry = new AsyncRetry(timeoutMs, executor);
        return retry.doRetry(callable, retryTimes, sleepTimeInMilliSecond, exponential, null);
    }

    /**
     * 创建异步执行的线程池。特性如下：
     * core大小为0，初始状态下无线程，无初始消耗。
     * max大小为5，最多五个线程。
     * 60秒超时时间，闲置超过60秒线程会被回收。
     * 使用SynchronousQueue，任务不会排队，必须要有可用线程才能提交成功，否则会RejectedExecutionException。
     *
     * @return 线程池
     */
    public static ThreadPoolExecutor createThreadPoolExecutor() {
        return new ThreadPoolExecutor(0, 5,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>());
    }


    private static class Retry {

        public <T> T doRetry(Callable<T> callable, int retryTimes, long sleepTimeInMilliSecond, boolean exponential, List<Class<?>> retryExceptionClasss)
                throws Exception {

            if (null == callable) {
                throw new IllegalArgumentException("系统编程错误, 入参callable不能为空 ! ");
            }

            if (retryTimes < 1) {
                throw new IllegalArgumentException(String.format(
                        "系统编程错误, 入参retrytime[%d]不能小于1 !", retryTimes));
            }

            Exception saveException = null;
            for (int i = 0; i < retryTimes; i++) {
                try {
                    return call(callable);
                } catch (Exception e) {
                    saveException = e;
                    if (i == 0) {
                        LOG.error(String.format("Exception when calling callable, 异常Msg:%s", saveException.getMessage()), saveException);
                    }
                    
                    if (null != retryExceptionClasss && !retryExceptionClasss.isEmpty()) {
                        boolean needRetry = false;
                        for (Class<?> eachExceptionClass : retryExceptionClasss) {
                            if (eachExceptionClass == e.getClass()) {
                                needRetry = true;
                                break;
                            }
                        }
                        if (!needRetry) {
                            throw saveException;
                        }
                    }
                    
                    if (i + 1 < retryTimes && sleepTimeInMilliSecond > 0) {
                        long startTime = System.currentTimeMillis();

                        long timeToSleep;
                        if (exponential) {
                            timeToSleep = sleepTimeInMilliSecond * (long) Math.pow(2, i);
                            if(timeToSleep >= MAX_SLEEP_MILLISECOND) {
                                timeToSleep = MAX_SLEEP_MILLISECOND;
                            }
                        } else {
                            timeToSleep = sleepTimeInMilliSecond;
                            if(timeToSleep >= MAX_SLEEP_MILLISECOND) {
                                timeToSleep = MAX_SLEEP_MILLISECOND;
                            }
                        }

                        try {
                            Thread.sleep(timeToSleep);
                        } catch (InterruptedException ignored) {
                        }

                        long realTimeSleep = System.currentTimeMillis()-startTime;

                        LOG.error(String.format("Exception when calling callable, 即将尝试执行第%s次重试.本次重试计划等待[%s]ms,实际等待[%s]ms, 异常Msg:[%s]",
                                i+1, timeToSleep,realTimeSleep, e.getMessage()));

                    }
                }
            }
            throw saveException;
        }

        protected <T> T call(Callable<T> callable) throws Exception {
            return callable.call();
        }
    }

    private static class AsyncRetry extends Retry {

        private long timeoutMs;
        private ThreadPoolExecutor executor;

        public AsyncRetry(long timeoutMs, ThreadPoolExecutor executor) {
            this.timeoutMs = timeoutMs;
            this.executor = executor;
        }

        /**
         * 使用传入的线程池异步执行任务，并且等待。
         * <p/>
         * future.get()方法，等待指定的毫秒数。如果任务在超时时间内结束，则正常返回。
         * 如果抛异常（可能是执行超时、执行异常、被其他线程cancel或interrupt），都记录日志并且网上抛异常。
         * 正常和非正常的情况都会判断任务是否结束，如果没有结束，则cancel任务。cancel参数为true，表示即使
         * 任务正在执行，也会interrupt线程。
         *
         * @param callable
         * @param <T>
         * @return
         * @throws Exception
         */
        @Override
        protected <T> T call(Callable<T> callable) throws Exception {
            Future<T> future = executor.submit(callable);
            try {
                return future.get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                LOG.warn("Try once failed", e);
                throw e;
            } finally {
                if (!future.isDone()) {
                    future.cancel(true);
                    LOG.warn("Try once task not done, cancel it, active count: " + executor.getActiveCount());
                }
            }
        }
    }

}
