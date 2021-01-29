package com.aktarma.retrylog.server.db.rocksdb;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompressionType;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.TxnDBWritePolicy;
import org.rocksdb.WriteOptions;

public class RocksTests {
	static {
		RocksDB.loadLibrary();
	}

	public TransactionDB getDb(String path) {

		try (Options options = new Options()) {

			// most of these options are suggested by
			// https://github.com/facebook/rocksdb/wiki/Set-Up-Options

			// general options
			options.setCreateIfMissing(true);
			options.setCompressionType(CompressionType.LZ4_COMPRESSION);
			options.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);
			options.setLevelCompactionDynamicLevelBytes(true);
			options.setMaxOpenFiles(30);
			options.setIncreaseParallelism(10);

//			options.setAllowMmapReads(true);
//			options.setAllowMmapWrites(true);
//			
//			options.setUseFsync(true);

			// key prefix for state node lookups
			//options.useFixedLengthPrefixExtractor(NodeKeyCompositor.PREFIX_BYTES);

			// table options
			final BlockBasedTableConfig tableCfg;
			options.setTableFormatConfig(tableCfg = new BlockBasedTableConfig());
			tableCfg.setBlockSize(16 * 1024);
			// tableCfg.setBlockCacheSize(32 * 1024 * 1024);
			tableCfg.setBlockCache(new LRUCache(32 * 1024 * 102));

			tableCfg.setCacheIndexAndFilterBlocks(true);
			tableCfg.setPinL0FilterAndIndexBlocksInCache(true);
			tableCfg.setFilterPolicy(new BloomFilter(10, false));

			TransactionDBOptions tx = new TransactionDBOptions();
			tx.setWritePolicy(TxnDBWritePolicy.WRITE_COMMITTED);

			try {
				return TransactionDB.open(options, tx, path);
			} catch (RocksDBException e) {
				throw new RuntimeException("Failed to initialize database", e);
			}
		}
	}

	private final WriteOptions wo = new WriteOptions();
	ReadOptions ro = new ReadOptions().setPrefixSameAsStart(true).setVerifyChecksums(true);

	@Test
	public void uniqunessTest() throws RocksDBException {

		try (TransactionDB db = getDb("/home/tests/rocks")) {

			try (Transaction tx = db.beginTransaction(wo)) {

				tx.put("0key".getBytes(), UUID.randomUUID().toString().getBytes());
				if (tx.get(ro, "0key".getBytes()) == null) {
					System.err.println("Key NOT found 1");
					tx.put("key".getBytes(), UUID.randomUUID().toString().getBytes());
				} else {
					System.err.println("Key found");
					tx.delete("0key".getBytes());
					if (tx.get(ro, "0key".getBytes()) == null) {
						System.err.println("Key NOT found 2 -- OK");
					}
				}
				tx.commit();
			}

			db.enableFileDeletions(true);

			db.compactRange();

//			RocksIterator it = db.newIterator();
//			it.seekToFirst();
//			while (it.isValid()) {
//				System.err.println(new String(it.key()) + " --> " + new String(it.value()));
//				it.next();
//			}

			AtomicLong l = new AtomicLong(0);

			AtomicLong start = new AtomicLong(System.currentTimeMillis());
			IntStream.range(0, 300_000_001).forEach(i -> {
				l.incrementAndGet();
				if (i > 0) {

					if (i % 10000 == 0) {

						System.err.print('.');
					}
					if (i % 1000000 == 0) {

						System.err.print('\n');

						long x = System.currentTimeMillis() - start.get();
						long tps = l.get() / (x / 1000);
						System.err.println("At: " + i + ", TPS:" + tps);

						l.set(0);
						try {
							db.syncWal();
						} catch (RocksDBException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						// db.openAsSecondary(options, path, secondaryPath)
//						try {
//							db.compactRange();
//						} catch (RocksDBException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
						start.set(System.currentTimeMillis());
					}
				}
				byte[] byteArray = uuid();

				try {
					putOne(db, byteArray, byteArray);
				} catch (RocksDBException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
//				 
//				RetryLogEntry initial = new RetryLogEntry(byteArray, 100, 120, 120, 0);
//
//				TransmissionBeginResponse.Code added = backend.begin(initial);
//				assertThat(added, is(TransmissionBeginResponse.Code.OK));
//
//				TransmissionBeginResponse.Code added2 = backend.begin(new RetryLogEntry(byteArray, 101, 121, 121, 1));
//				assertThat(added2, is(TransmissionBeginResponse.Code.INFLIGHT));
//
//				OpResult code = backend.confirm(initial);
//				assertThat(code.getCode(), is(OperationResultCode.OK));
			});
		}
	}

	public void putOne(TransactionDB db, byte[] key, byte[] value) throws RocksDBException {
//		WriteOptions writeOpts = new WriteOptions();
//		writeOpts.setSync(true);
//		writeOpts.setDisableWAL(false);
		try (Transaction tx = db.beginTransaction(wo)) {
			tx.put(key, value);
			if (tx.get(ro, key) == null) {
				System.err.println("Key NOT found");
				tx.put(key, value);
			}
			// tx.delete(key);
			tx.commit();
		}
	}

	static byte[] uuid() {
		try {
			return Hex.decodeHex(UUID.randomUUID().toString().replace("-", ""));
		} catch (DecoderException e) {
			throw new RuntimeException(e);
		}
	}
}
