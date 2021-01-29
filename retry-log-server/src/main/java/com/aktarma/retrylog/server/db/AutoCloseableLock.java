package com.aktarma.retrylog.server.db;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class AutoCloseableLock implements AutoCloseable, Lock {

	private final Lock lock;

	public AutoCloseableLock(Lock lock) {
		super();
		this.lock = lock;
	}

	public void lock() {
		lock.lock();
	}

	public void lockInterruptibly() throws InterruptedException {
		lock.lockInterruptibly();
	}

	public boolean tryLock() {
		return lock.tryLock();
	}

	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		return lock.tryLock(time, unit);
	}

	public void unlock() {
		lock.unlock();
	}

	public Condition newCondition() {
		return lock.newCondition();
	}

	@Override
	public void close() throws Exception {
		lock.unlock();
	}

}
