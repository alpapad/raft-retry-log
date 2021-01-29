package com.aktarma.retrylog.server.db.rocksdb;

import java.util.ArrayList;
import java.util.List;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksObject;
import org.rocksdb.Statistics;

public class RkObject {
	private final List<RocksObject> objects = new ArrayList<>();

	public Checkpoint newCheckpoint(final RocksDB rocksDb) {
		return reg(Checkpoint.create(rocksDb));
	}
	public DBOptions newDBOptions() {
		return reg(new DBOptions());
	}

	public ColumnFamilyDescriptor newColumnFamilyDescriptor(final byte[] columnFamilyName, final ColumnFamilyOptions columnFamilyOptions) {
		return new ColumnFamilyDescriptor(columnFamilyName, columnFamilyOptions);
	}

	public Statistics newStatistics() {
		return reg(new Statistics());
	}

	public ColumnFamilyOptions newColumnFamilyOptions() {
		return reg(new ColumnFamilyOptions());
	}

	public BlockBasedTableConfig newBlockBasedTableConfig() {
		return new BlockBasedTableConfig();
	}

	public LRUCache newLRUCache(final long capacity) {
		return reg(new LRUCache(capacity));
	}
	
	public BloomFilter  newBloomFilter(final double bitsPerKey, final boolean useBlockBasedMode) {
		return reg(new BloomFilter(bitsPerKey, useBlockBasedMode));
	}
	

	public void close() {
		objects.forEach(RocksObject::close);
		objects.clear();
	}
	
	public <T extends RocksObject> T reg(T ref) {
		objects.add(ref);
		return ref;
	}
}
