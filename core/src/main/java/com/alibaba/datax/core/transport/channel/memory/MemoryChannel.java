package com.alibaba.datax.core.transport.channel.memory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.record.TerminateRecord;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 内存Channel的具体实现，底层其实是一个ArrayBlockingQueue
 *
 */
public class MemoryChannel extends Channel {

	private int bufferSize = 0;

	private AtomicInteger memoryBytes = new AtomicInteger(0);

	private ArrayBlockingQueue<Record> queue = null;

	private ReentrantLock lock;

	private Condition notInsufficient, notEmpty;

	public MemoryChannel(final Configuration configuration) {
		super(configuration);
		this.queue = new ArrayBlockingQueue<Record>(this.getCapacity());
		this.bufferSize = configuration.getInt(CoreConstant.DATAX_CORE_TRANSPORT_EXCHANGER_BUFFERSIZE);

		lock = new ReentrantLock();
		notInsufficient = lock.newCondition();
		notEmpty = lock.newCondition();
	}

	@Override
	public void close() {
		super.close();
		try {
			this.queue.put(TerminateRecord.get());
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public void clear(){
		this.queue.clear();
	}

	@Override
	protected void doPush(Record r) {
		try {
			long startTime = System.nanoTime();
			this.queue.put(r);
			waitWriterTime += System.nanoTime() - startTime;
            memoryBytes.addAndGet(r.getMemorySize());
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	protected void doPushAll(Collection<Record> rs) {
		try {
			long startTime = System.nanoTime();
			lock.lockInterruptibly();
			int bytes = getRecordBytes(rs);
			while (memoryBytes.get() + bytes > this.byteCapacity || rs.size() > this.queue.remainingCapacity()) {
				notInsufficient.await(200L, TimeUnit.MILLISECONDS);
            }
			this.queue.addAll(rs);
			waitWriterTime += System.nanoTime() - startTime;
			memoryBytes.addAndGet(bytes);
			notEmpty.signalAll();
		} catch (InterruptedException e) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.RUNTIME_ERROR, e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	protected Record doPull() {
		try {
			long startTime = System.nanoTime();
			Record r = this.queue.take();
			waitReaderTime += System.nanoTime() - startTime;
			memoryBytes.addAndGet(-r.getMemorySize());
			return r;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
	}

	@Override
	protected void doPullAll(Collection<Record> rs) {
		assert rs != null;
		rs.clear();
		try {
			long startTime = System.nanoTime();
			lock.lockInterruptibly();
			while (this.queue.drainTo(rs, bufferSize) <= 0) {
				notEmpty.await(200L, TimeUnit.MILLISECONDS);
			}
			waitReaderTime += System.nanoTime() - startTime;
			int bytes = getRecordBytes(rs);
			memoryBytes.addAndGet(-bytes);
			notInsufficient.signalAll();
		} catch (InterruptedException e) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.RUNTIME_ERROR, e);
		} finally {
			lock.unlock();
		}
	}

	private int getRecordBytes(Collection<Record> rs){
		int bytes = 0;
		for(Record r : rs){
			bytes += r.getMemorySize();
		}
		return bytes;
	}

	@Override
	public int size() {
		return this.queue.size();
	}

	@Override
	public boolean isEmpty() {
		return this.queue.isEmpty();
	}

}
