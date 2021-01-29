package com.aktarma.retrylog.server.db;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RwLock {
	public static final Logger LOG = LoggerFactory.getLogger(RwLock.class);

	StampedLock lock1 = new StampedLock();

	public RwLock() {
	}

	public AutoCloseableLock readLock() {
		return new AutoCloseableLock(lock1.asReadLock());
	}

	public AutoCloseableLock writeLock() {
		return new AutoCloseableLock(lock1.asWriteLock());
	}

	public static class AutoCloseableLock implements AutoCloseable, Lock {

		private final Lock lock;

		public AutoCloseableLock(Lock lock) {
			super();
			this.lock = lock;
			try {
				this.lock.lockInterruptibly();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
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
		public void close() {
			lock.unlock();
		}

	}

}
