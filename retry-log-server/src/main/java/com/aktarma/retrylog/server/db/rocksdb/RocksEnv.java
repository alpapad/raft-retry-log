package com.aktarma.retrylog.server.db.rocksdb;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.ratis.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.ratis.thirdparty.com.google.common.collect.ImmutableMap;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.HashLinkedListMemTableConfig;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aktarma.retrylog.server.db.StorageException;
import com.aktarma.retrylog.server.db.utils.TarUtils;

public class RocksEnv {
	private static final Logger LOG = LoggerFactory.getLogger(RocksEnv.class);

	private final static String TRANSACTION_DB = "transaction";
	private final static String DUP_DB = "dupdb";
	private final static String TIMER_DB = "timer";
	private final static String SNAPSHOT_DB = "snapshot";
	private final static String DEFAULT_DB = "default";
	private final List<String> dbNs = ImmutableList.of(TRANSACTION_DB, DUP_DB, TIMER_DB, SNAPSHOT_DB, DEFAULT_DB);

	private final ColumnFamilyHandle transactDbHandle;
	private final ColumnFamilyHandle dupDbHandle;
	private final ColumnFamilyHandle timerDbHandle;
	private final ColumnFamilyHandle snapshotDbHandle;

	private final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

	private final RocksDB db;
	private final Map<String, ColumnFamilyHandle> columnHandlesByName;

	private final RocksDbi transactions;
	private final RocksDbi duplicates;
	private final RocksDbi timer;
	private final RocksDbi snapshot;

	private final RkObject rkObject = new RkObject();
	private final Statistics stats = rkObject.newStatistics();

	private final Checkpoint checkpoint;

	private final List<RocksDbi> dbis;

	private final BlockBasedTableConfig tableCfg = rkObject.newBlockBasedTableConfig()
			// .setBlockSize(16 * 1024)
			.setBlockCache(rkObject.newLRUCache(8 * SizeUnit.MB))
			// .setCacheIndexAndFilterBlocks(true)//
			// .setPinL0FilterAndIndexBlocksInCache(true)
			.setFilterPolicy(rkObject.newBloomFilter(10, false));

	private final ColumnFamilyOptions cfOpts = rkObject.newColumnFamilyOptions()//
			.setMemTableConfig(new HashLinkedListMemTableConfig())//
			// .setCompressionType(CompressionType.NO_COMPRESSION)//
			.setTableFormatConfig(tableCfg)//
			.setTargetFileSizeBase(256 * SizeUnit.MB)//
			.setInplaceUpdateSupport(true)
			//
			.setCompressionType(CompressionType.LZ4_COMPRESSION) //
			.setCompactionStyle(CompactionStyle.LEVEL) //
			.optimizeLevelStyleCompaction()

			// https://github.com/facebook/rocksdb/pull/5744
			.setForceConsistencyChecks(true);

	public RocksEnv(String path) {
		try {
			final List<ColumnFamilyDescriptor> columnDescriptors = dbNs.stream()
					.map(n -> rkObject.newColumnFamilyDescriptor(n.getBytes(StandardCharsets.UTF_8), cfOpts))
					.collect(Collectors.toList());

			stats.setStatsLevel(StatsLevel.ALL);

			final List<ColumnFamilyHandle> columnHandles = new ArrayList<>(columnDescriptors.size());

			DBOptions dbOpts = rkObject.newDBOptions()//
					.setStatistics(stats)//
					.setAllowConcurrentMemtableWrite(false)// Concurrent memtable write is not supported for hash linked
															// list memtable
					.setMaxOpenFiles(-1)//
					.setCreateIfMissing(true)//
					.setCreateMissingColumnFamilies(true)//
					.setMaxBackgroundJobs(4)//
					.setIncreaseParallelism(4) //
					.setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL)//
					.setEnv(Env.getDefault().setBackgroundThreads(10));

			db = RocksDB.open(dbOpts, /* txOptions, */ path, columnDescriptors, columnHandles);
			db.enableAutoCompaction(columnHandles);
			db.enableFileDeletions(true);

			checkpoint = rkObject.newCheckpoint(db);

			final ImmutableMap.Builder<String, ColumnFamilyHandle> builder = ImmutableMap.builder();

			for (ColumnFamilyHandle columnHandle : columnHandles) {
				builder.put(new String(columnHandle.getName(), StandardCharsets.UTF_8), columnHandle);
			}
			columnHandlesByName = builder.build();
		} catch (final RocksDBException e) {
			LOG.error("Error initializing rockdb environment", e);
			rkObject.close();
			throw new StorageException(e);
		}

		transactDbHandle = columnHandlesByName.get(TRANSACTION_DB);
		dupDbHandle = columnHandlesByName.get(DUP_DB);
		timerDbHandle = columnHandlesByName.get(TIMER_DB);
		snapshotDbHandle = columnHandlesByName.get(SNAPSHOT_DB);
		
		transactions = new RocksDbi(db, transactDbHandle);
		duplicates = new RocksDbi(db, dupDbHandle);
		timer = new RocksDbi(db, timerDbHandle);
		snapshot = new RocksDbi(db, snapshotDbHandle);
		
		dbis = Arrays.asList(transactions, duplicates, timer, snapshot,
				new RocksDbi(db, columnHandlesByName.get("default")));
	}

	public RocksDbi getTransactions() {
		return transactions;
	}

	public RocksDbi getDuplicates() {
		return duplicates;
	}

	public RocksDbi getTimer() {
		return timer;
	}

	public RocksDbi getSnapshot() {
		return snapshot;
	}

	public void close() {
		rkObject.close();

		dbis.forEach(RocksDbi::close);
		columnFamilyHandles.clear();

		try {
			db.syncWal();
		} catch (RocksDBException e) {
			LOG.error("Error syncing rockdb environment", e);
		}
		try {
			db.closeE();
		} catch (RocksDBException e) {
			LOG.error("Error clossing rockdb environment", e);
		}
	}

	public RocksTransaction txnWrite() {
		return new RocksTransaction(db);
	}

	public RocksTransaction txnRead() {
		return new RocksTransaction(db);
	}

	public void sync(boolean b) {
		try {
			this.db.syncWal();
			this.db.compactRange();
			this.columnFamilyHandles.stream().forEach(h -> {
				try {
					db.compactRange(h);
				} catch (RocksDBException e) {
					LOG.error("Error compacting range rockdb environment for dbi {} ", h, e);
					throw new StorageException(e);
				}
			});
		} catch (RocksDBException e) {
			LOG.error("Error compacting range rockdb environment", e);
			throw new StorageException(e);
		}
	}

	public CompletableFuture<Void> checkpoint(String checkpointDir, File snapshotFile) {
		sync(true);
		File cckp = new File(checkpointDir);
		// this needs to be sync
		try {
			checkpoint.createCheckpoint(checkpointDir);
		} catch (RocksDBException e) {
			FileUtils.deleteQuietly(cckp);
			LOG.error("Error creating checkpoint in {} ", checkpointDir, e);
			throw new StorageException(e);
		}

		return CompletableFuture.supplyAsync(() -> {
			try {
				TarUtils.createTarFolder(cckp.toPath(), snapshotFile.getAbsolutePath());
				FileUtils.deleteQuietly(cckp);
			} catch (IOException e) {
				FileUtils.deleteQuietly(snapshotFile);
				FileUtils.deleteQuietly(cckp);
				LOG.error("Error copying  checkpoint in {} to ", checkpointDir, snapshotFile, e);
				throw new StorageException(e);
			}
			return null;
		});
	}

	public String stat() {
		return stats.toString();
	}
}
