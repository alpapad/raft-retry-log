package com.aktarma.retrylog.server.db.rocksdb;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatchWithIndex;
import org.rocksdb.WriteOptions;

public class RocksTransaction implements AutoCloseable {
	private final WriteBatchWithIndex write;
	private final RocksDB db;
	private final WriteOptions wo = new WriteOptions();

	public RocksTransaction(RocksDB db) {
		super();
		this.write = new WriteBatchWithIndex();
		this.db = db;
	}

	@Override
	public void close() {
		this.write.close();
		this.wo.close();
	}

	public void delete(ColumnFamilyHandle handle, byte[] key) throws RocksDBException {
		write.delete(handle, key);
	}

	public byte[] get(ColumnFamilyHandle handle, ReadOptions readOpts, byte[] key) throws RocksDBException {
		return db.get(handle, key);
	}

	public void put(ColumnFamilyHandle handle, byte[] key, byte[] value) throws RocksDBException {
		write.put(handle, key, value);
	}

	public void commit() throws RocksDBException {
		db.write(wo, write);
	}

	public void rollback() {
		write.clear();
	}
}
