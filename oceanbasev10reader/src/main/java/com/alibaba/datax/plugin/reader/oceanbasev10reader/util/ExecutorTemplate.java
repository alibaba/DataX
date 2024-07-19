package com.alibaba.datax.plugin.reader.oceanbasev10reader.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutorTemplate<T> {

    /**
     * The default thread pool size. Set as the number of available processors by default.
     */
    public static int DEFAULT_POOL_SIZE = Runtime.getRuntime().availableProcessors();

    /**
     * Indicate whether the executor closes automatically.
     */
    private final boolean autoClose;

    /**
     *
     */
    private final List<Future<T>> futures;

    /**
     *
     */
    private final ExecutorService internalExecutor;

    private final ExecutorCompletionService<T> completionService;

    /**
     * Set pool size for ExecutorTemplate.
     */
    public static void setPoolSize(int size) {
        DEFAULT_POOL_SIZE = size;
    }

    /**
     * Default: 1024 AutoClose: true
     *
     * @param poolName
     */
    public ExecutorTemplate(String poolName) {
        this(defaultExecutor(poolName), true);
    }

    /**
     * Default: 1024 AutoClose: true
     *
     * @param poolName
     */
    public ExecutorTemplate(String poolName, int poolSize) {
        this(defaultExecutor(poolName, poolSize), true);
    }

    public ExecutorTemplate(String poolName, int poolSize, boolean autoClose) {
        this(defaultExecutor(poolName, poolSize), autoClose);
    }

    /**
     * Default: 1024
     *
     * @param poolName
     * @param autoClose
     */
    public ExecutorTemplate(String poolName, boolean autoClose) {
        this(defaultExecutor(poolName), autoClose);
    }

    /**
     * Default: 1024 AutoClose: true
     *
     * @param executor
     */
    public ExecutorTemplate(ExecutorService executor) {
        this(executor, true);
    }

    /**
     * @param executor
     */
    public ExecutorTemplate(ExecutorService executor, boolean autoClose) {
        this.autoClose = autoClose;
        this.internalExecutor = executor;
        this.completionService = new ExecutorCompletionService<>(executor);
        this.futures = Collections.synchronizedList(new ArrayList<>());
    }

    /**
     * @param poolName
     * @return ExecutorService
     */
    public static ExecutorService defaultExecutor(String poolName) {
        return defaultExecutor(100000, poolName, DEFAULT_POOL_SIZE);
    }

    /**
     * @param poolName
     * @param poolSize
     * @return ExecutorService
     */
    public static ExecutorService defaultExecutor(String poolName, int poolSize) {
        return defaultExecutor(100000, poolName, poolSize);
    }

    /**
     * @param capacity
     * @param poolName
     * @return ExecutorService
     */
    public static ExecutorService defaultExecutor(int capacity, String poolName, int poolSize) {
        return new ThreadPoolExecutor(poolSize, poolSize, 30, TimeUnit.SECONDS, /* */
                new ArrayBlockingQueue<>(capacity), new NamedThreadFactory(poolName));
    }

    /**
     * Submit a callable task
     *
     * @param task
     */
    public void submit(Callable<T> task) {
        Future<T> f = this.completionService.submit(task);
        futures.add(f);
        check(f);
    }

    /**
     * Submit a runnable task
     *
     * @param task
     */
    public void submit(Runnable task) {
        Future<T> f = this.completionService.submit(task, null);
        futures.add(f);
        check(f);
    }

    /**
     * Wait all the task run finished, and get all the results.
     *
     * @return List<T>
     */
    public List<T> waitForResult() {
        try {
            int index = 0;
            Throwable ex = null;
            List<T> result = new ArrayList<T>();
            while (index < futures.size()) {
                try {
                    Future<T> f = this.completionService.take();
                    result.add(f.get());
                } catch (Throwable e) {
                    ex = getRootCause(e);
                    break;
                }
                index++;
            }
            if (ex != null) {
                cancelAll();
                throw new RuntimeException(ex);
            } else {
                return result;
            }
        } finally {
            clearFutures();
            if (autoClose) {
                destroyExecutor();
            }
        }
    }

    /**
     *
     */
    public void cancelAll() {
        for (Future<T> f : futures) {
            if (!f.isDone() && !f.isCancelled()) {
                f.cancel(false);
            }
        }
    }

    /**
     *
     */
    public void clearFutures() {
        this.futures.clear();
    }

    /**
     *
     */
    public void destroyExecutor() {
        if (internalExecutor != null && !internalExecutor.isShutdown()) {
            this.internalExecutor.shutdown();
            try {
                this.internalExecutor.awaitTermination(0, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
        }
    }

    /**
     * Fast check the future
     *
     * @param f
     */
    private void check(Future<T> f) {
        if (f != null && f.isDone()) {
            try {
                f.get();
            } catch (Throwable e) {
                cancelAll();
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * @param throwable
     * @return Throwable
     */
    private Throwable getRootCause(Throwable throwable) {
        final Throwable holder = throwable;
        final List<Throwable> list = new ArrayList<>();
        while (throwable != null && !list.contains(throwable)) {
            list.add(throwable);
            throwable = throwable.getCause();
        }
        return list.size() < 2 ? holder : list.get(list.size() - 1);
    }

    /**
     * An internal named thread factory
     */
    static class NamedThreadFactory implements ThreadFactory {

        /**
         *
         */
        private final boolean daemon;

        /**
         *
         */
        private final String name;

        /**
         *
         */
        private final AtomicInteger seq = new AtomicInteger(0);

        /**
         * @param name
         */
        public NamedThreadFactory(String name) {
            this(name, false);
        }

        /**
         * @param name
         * @param daemon
         */
        public NamedThreadFactory(String name, boolean daemon) {
            this.name = name;
            this.daemon = daemon;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(daemon);
            t.setPriority(Thread.NORM_PRIORITY);
            t.setName((name + seq.incrementAndGet()));
            return t;
        }
    }
}