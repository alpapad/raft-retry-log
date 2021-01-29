package com.aktarma.retrylog.server.db.rocksdb;

import java.util.Arrays;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import com.aktarma.retrylog.server.db.StorageException;

public class RocksDbi {

	private final RocksDB db;
	private final ColumnFamilyHandle handle;
	private ReadOptions readOpts = new ReadOptions().setPrefixSameAsStart(true).setVerifyChecksums(false);

	public RocksDbi(RocksDB db, ColumnFamilyHandle handle) {
		this.db = db;
		this.handle = handle;//
	}

	public boolean putIfAbsent(RocksTransaction tx, byte[] key, byte[] value) {
		try {
			if (tx.get(handle, readOpts, key) == null) {
				tx.put(handle, key, value);
				return true;
			}
		} catch (RocksDBException ex) {
			throw new StorageException(ex);
		}

		return false;
	}

	public boolean put(RocksTransaction tx, byte[] key, byte[] value)  {
		try {
			tx.put(handle, key, value);
		} catch (RocksDBException ex) {
			throw new StorageException(ex);
		}
		return true;
	}
	
	public byte[] get(RocksTransaction tx, byte[] key) {
		try {
			return tx.get(handle, readOpts, key);
		} catch (RocksDBException ex) {
			throw new StorageException(ex);
		}
	}
	
	public boolean delete(RocksTransaction tx, byte[] key) {
		try {
			tx.delete(handle, key);
		} catch (RocksDBException ex) {
			throw new StorageException(ex);
		}
		return true;
	}

	public boolean delete(RocksTransaction tx, byte[] key, byte[] value) {
		try {
			byte[] val = tx.get(handle, readOpts, key);
			if(val == null || !Arrays.equals(val, value)) {
				return false;
			}
			tx.delete(handle, key);
		} catch (RocksDBException ex) {
			throw new StorageException(ex);
		}
		return true;
	}
	
	public void truncate() {
		try (final RocksIterator rocksIterator = db.newIterator(handle)) {
			rocksIterator.seekToFirst();
			if (rocksIterator.isValid()) {
				final byte[] firstKey = rocksIterator.key();
				rocksIterator.seekToLast();
				if (rocksIterator.isValid()) {
					final byte[] lastKey = rocksIterator.key();
					db.deleteRange(handle, firstKey, lastKey);
					db.delete(handle, lastKey);
				}
			}
		} catch (final RocksDBException e) {
			throw new StorageException(e);
		}
	}

	public void close() {
		this.readOpts.close();
		this.handle.close();
	}
}
