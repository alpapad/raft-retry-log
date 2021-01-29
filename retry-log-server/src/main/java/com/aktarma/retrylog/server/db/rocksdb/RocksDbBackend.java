package com.aktarma.retrylog.server.db.rocksdb;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.common.primitives.Longs;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.MD5FileUtil;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aktarma.retrylog.common.wire.proto.TransmissionBeginRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionBeginResponse;
import com.aktarma.retrylog.common.wire.proto.TransmissionBeginResponse.Code;
import com.aktarma.retrylog.common.wire.proto.TransmissionCommitRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionConfirmedRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionEntry;
import com.aktarma.retrylog.common.wire.proto.TransmissionFailRequest;
import com.aktarma.retrylog.server.db.IDbBackend;
import com.aktarma.retrylog.server.db.OpResult;
import com.aktarma.retrylog.server.db.OperationResultCode;
import com.aktarma.retrylog.server.db.RwLock;
import com.aktarma.retrylog.server.db.RwLock.AutoCloseableLock;
import com.aktarma.retrylog.server.db.StorageException;
import com.aktarma.retrylog.server.db.TermIndexSupplier;
import com.aktarma.retrylog.server.db.utils.TarUtils;

public class RocksDbBackend implements IDbBackend, StateMachineStorage {
	static {
		RocksDB.loadLibrary();
	}
	private static final Logger LOG = LoggerFactory.getLogger(RocksDbBackend.class);

	private RocksEnv env;

	private RocksDbi duplicatesDbi;
	private RocksDbi transactionDbi;
	private RocksDbi timerDbi;
	private RocksDbi snapshotDbi;
	private final RwLock lock = new RwLock();

	private final static byte[] TERM_KEY = Longs.toByteArray(0l);
	private final static byte[] INDEX_KEY = Longs.toByteArray(1l);

	private final TermIndexSupplier supplier;

	private volatile SingleFileSnapshotInfo currentSnapshot = null;

	private RaftStorage raftStorage;
	private File smDir = null;
	private File rocksDbDir = null;
	private RocksStorageHelper helper;

	public RocksDbBackend(TermIndexSupplier supplier) {
		super();
		this.supplier = supplier;
	}

	private final void startDb() {
		this.currentSnapshot = helper.findLatestSnapshot();

		boolean shouldLoadFromSnapshot;
		if (currentSnapshot == null) {
			initialize();
			shouldLoadFromSnapshot = checkForNewerSnapshot(null);
		} else if (new File(this.rocksDbDir,"IDENTITY").exists()) {
			initialize();
			shouldLoadFromSnapshot = checkForNewerSnapshot(currentSnapshot.getTermIndex());
		} else {
			shouldLoadFromSnapshot = true;
		}

		if (shouldLoadFromSnapshot) {
			close();
			try {
				TarUtils.extractTarToFolder(currentSnapshot.getFile().getPath().toString(), rocksDbDir);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			initialize();
		}
	}

	@Override
	public void init(RaftStorage raftStorage) throws IOException {
		this.helper = new RocksStorageHelper(raftStorage.getStorageDir().getStateMachineDir());
		this.raftStorage = raftStorage;
		this.smDir = this.raftStorage.getStorageDir().getStateMachineDir();
		this.rocksDbDir = new File(smDir, "rocksdb/");
		this.rocksDbDir.mkdirs();
		this.currentSnapshot = helper.findLatestSnapshot();
		startDb();
	}

	@Override
	public void reinitialize() {
		startDb();
	}

	private boolean checkForNewerSnapshot(TermIndex snapshotIndex) {
		TermIndex dbIndex = this.getSnapshotInfo();
		if (snapshotIndex == null) {
			if (dbIndex.getTerm() <= 0 && dbIndex.getTerm() <= 0) {
				// new db
				return false;
			}
			this.takeSnapshot(dbIndex);
			return true;
		} else {
			// if db index is higher than last saved index, then snapshot current
			if (dbIndex.getTerm() > snapshotIndex.getTerm() || dbIndex.getIndex() > snapshotIndex.getIndex()) {
				this.takeSnapshot(dbIndex);
				return true;
			}
		}
		return false;
	}

	public void initialize() {
		close();

		env = new RocksEnv(rocksDbDir.toString());
		// keeps exactly one version of the message id (the latest)
		transactionDbi = env.getTransactions();

		// keeps exactly one timer of the message id (the latest) but we might have
		// multiple message ids under the same key
		timerDbi = env.getTimer();

		// keeps exactly one dedup instance
		duplicatesDbi = env.getDuplicates();
		snapshotDbi = env.getSnapshot();

		try (RocksTransaction txn = env.txnWrite()) {
			if (null == snapshotDbi.get(txn, TERM_KEY)) {
				snapshotDbi.put(txn, TERM_KEY, Longs.toByteArray(0));
			}

			if (null == snapshotDbi.get(txn, INDEX_KEY)) {
				snapshotDbi.put(txn, INDEX_KEY, Longs.toByteArray(RaftLog.INVALID_LOG_INDEX));
			}
			txn.commit();
		} catch (RocksDBException e) {
			throw new StorageException(e);
		}
	}

	@Override
	public boolean sync() {
		storeSnapshotInfo(this.supplier.getLastAppliedTermIndex());
		env.sync(true);
		return true;
	}

	@Override
	public void info() {
		System.err.println("\tEnvironment  : " + env.stat());
	}

	@Override
	public TransmissionBeginResponse.Code begin(long term, long index, TransmissionBeginRequest request) {
		RocksDbRetryLogEntry entry = RocksDbRetryLogEntry.from(term, index, request);
		byte[] transactionKey = entry.getTransactionKey();
		byte[] transactionValue = entry.asTransactionValue();

		try (AutoCloseableLock l = lock.readLock(); RocksTransaction txn = env.txnWrite()) {
			storeSnapshotInfo(txn, term, index);
			// If it is the first time we see this message, then we do a dedup and then we
			// try to insert.
			if (entry.getRetry() == 0) {
				// We assume the entry will not be there

				if (storeDedup(txn, transactionKey, entry)) {
					if (transactionDbi.putIfAbsent(txn, transactionKey, transactionValue)) {// ,
																							// PutFlags.MDB_NOOVERWRITE))
																							// {
						addRetryTimer(txn, transactionKey, entry, null);
						txn.commit();
						return Code.OK;
					} else {
						// SHOULD NOT HAPPEN
						txn.rollback();
						return Code.INFLIGHT;
					}
				} else {
					txn.rollback();
					return Code.DUPLICATE;
				}
			} else {
				// If in a retry, then we have old records in the db. Try to load it
				// MUST not be null..
				RocksDbRetryLogEntry previous = loadPrevious(txn, transactionKey, entry);
				if (previous == null) {
					txn.rollback();
					return Code.NOT_FOUND;
				}
				// if old value is stale, overwrite
				if (previous.getBeginPhaseExpires() < entry.getRefTime()) {
					// delete old version if it matches exactly (it should)
					// transactionDbi.delete(txn, key, previous.asMessageIdValue());

					if (transactionDbi.put(txn, transactionKey, transactionValue)) {
						addRetryTimer(txn, transactionKey, entry, previous);
						txn.commit();
						return Code.OK;
					}
				}
				LOG.debug("Message id {} exists, expires {} while ref-time is {} ", new String(entry.getMessageId()),
						previous.getBeginPhaseExpires(), entry.getRefTime());
				txn.rollback();
				return Code.INFLIGHT;
			}
		} catch (RocksDBException e) {
			throw new StorageException(e);
		}
	}

	@Override
	public OpResult commit(long term, long index, TransmissionCommitRequest request) {
		RocksDbRetryLogEntry entry = RocksDbRetryLogEntry.from(term, index, request);
		byte[] key = entry.getTransactionKey();

		try (AutoCloseableLock l = lock.readLock(); RocksTransaction txn = env.txnWrite()) {
			storeSnapshotInfo(txn, term, index);
			RocksDbRetryLogEntry prev = loadPrevious(txn, key, entry);

			if (prev == null) {
				LOG.error("Error, we should have a previous value.. for {}", entry);
				txn.rollback();
				return toOpResult(prev, OperationResultCode.NOT_FOUND);
			}

			prev.setBeginPhaseExpires(entry.getExpires());
			prev.setRefTime(entry.getRefTime());

			if (transactionDbi.put(txn, key, prev.asTransactionValue())) {
				txn.commit();
				return toOpResult(prev, OperationResultCode.OK);
			} else {
				LOG.debug("Message id {} exists, expires {} while ref-time is {} ", //
						new String(entry.getMessageId()), entry.getExpires(), entry.getRefTime());
				return toOpResult(prev, OperationResultCode.UPDATE_ERROR);
			}
		} catch (RocksDBException e) {
			throw new StorageException(e);
		}
	}

	@Override
	public OpResult fail(long term, long index, TransmissionFailRequest request) {
		RocksDbRetryLogEntry entry = RocksDbRetryLogEntry.from(term, index, request);

		byte[] key = entry.getTransactionKey();

		try (AutoCloseableLock l = lock.readLock(); RocksTransaction txn = env.txnWrite()) {
			storeSnapshotInfo(txn, term, index);
			RocksDbRetryLogEntry prev = loadPrevious(txn, key, entry);
			if (prev == null) {
				LOG.error("Error, we should have a previous value.. for {}", entry);
				txn.rollback();
				return toOpResult(prev, OperationResultCode.NOT_FOUND);
			}
			prev.setBeginPhaseExpires(0);
			prev.setRefTime(entry.getRefTime());

			if (transactionDbi.put(txn, key, prev.asTransactionValue())) {
				txn.commit();
				return toOpResult(prev, OperationResultCode.OK);
			} else {
				LOG.debug("Message id {} exists, expires {} while ref-time is {} ", //
						new String(entry.getMessageId()), entry.getExpires(), entry.getRefTime());
				return toOpResult(prev, OperationResultCode.UPDATE_ERROR);
			}
		} catch (RocksDBException e) {
			throw new StorageException(e);
		}
	}

	@Override
	public OpResult confirm(long term, long index, TransmissionConfirmedRequest request) {
		RocksDbRetryLogEntry entry = RocksDbRetryLogEntry.from(term, index, request);
		byte[] key = entry.getTransactionKey();

		try (AutoCloseableLock l = lock.readLock(); RocksTransaction txn = env.txnWrite()) {
			storeSnapshotInfo(txn, term, index);
			RocksDbRetryLogEntry prev = loadPrevious(txn, key, entry);
			if (prev == null) {
				LOG.error("Confirm: we should have a previous value for {}. Assuming it was confirmed before", entry);
				txn.rollback();
				return toOpResult(prev, OperationResultCode.NOT_FOUND);
			}
//			prev.setBeginPhaseExpires(0);
//			prev.setRefTime(entry.getRefTime());

			if (!transactionDbi.delete(txn, key)) {
				LOG.error("Did not find message ins transactions log: {}", entry);
			}

			if (!timerDbi.delete(txn, prev.getTimerKey())) {
				LOG.error("Did not find message in timers log: {}", entry);
			}

			txn.commit();
			return toOpResult(prev, OperationResultCode.OK);
		} catch (RocksDBException e) {
			throw new StorageException(e);
		}
	}

//	@Override
//	public OpResult discard(RetryLogEntry entry) {
//		ByteBuffer key = entry.getMsgIdKey();
//
//		try (AutoCloseableLock l = lock.readLock(); Transaction txn = env.begin()) {
//			storeSnapshotInfo(txn);
//
//			RetryLogEntry prev = loadPrevious(txn, key, entry);
//			if (prev == null) {
//				LOG.error("Error, we should have a previous value.. for {}", entry);
//				txn.rollback();
//				return new OpResult(prev, OperationResultCode.NOT_FOUND);
//			}
//			prev.setBeginPhaseExpires(0);
//			prev.setRefTime(entry.getRefTime());
//
//			if (!transactionDbi.delete(txn, key)) {
//				LOG.error("Did not find message ins transactions log: {}", entry);
//			}
//			if (!timerDbi.delete(txn, prev.getTimerKey(), prev.asTimerValue())) {
//				LOG.error("Did not find message in timers log: {}", entry);
//			}
//
//			txn.commit();
//			return new OpResult(prev, OperationResultCode.OK);
//		}
//	}

	@Override
	public TermIndex getSnapshotInfo() {
		long term = 0;
		long index = RaftLog.INVALID_LOG_INDEX;
		try (AutoCloseableLock l = lock.readLock(); RocksTransaction txn = env.txnRead()) {
			byte[] val = snapshotDbi.get(txn, TERM_KEY);
			ByteBuffer bb = ByteBuffer.wrap(val);
			term = bb.getLong();
			val = snapshotDbi.get(txn, INDEX_KEY);
			bb = ByteBuffer.wrap(val);
			index = bb.getLong();
		}
		return TermIndex.valueOf(term, index);
	}

	@Override
	public void close() {
		if (env != null) {
			env.close();
			env = null;
		}
	}

	private OpResult toOpResult(RocksDbRetryLogEntry prev, OperationResultCode code) {
		return new OpResult(TransmissionEntry.newBuilder()//
				.setNetworkref(ByteString.copyFrom(prev.getMessageId()))//
				.setRetry(prev.getRetry()).build(), //
				code);
	}

	private void storeSnapshotInfo(RocksTransaction txn, long term, long index) {
		snapshotDbi.put(txn, TERM_KEY, Longs.toByteArray(term));
		snapshotDbi.put(txn, INDEX_KEY, Longs.toByteArray(index));
	}

	private void storeSnapshotInfo(TermIndex idx) {
		try (RocksTransaction txn = env.txnWrite()) {
			storeSnapshotInfo(txn, idx.getTerm(), idx.getIndex());
			txn.commit();
		} catch (RocksDBException e) {
			throw new StorageException(e);
		}
	}

	private RocksDbRetryLogEntry loadPrevious(RocksTransaction txn, byte[] key, RocksDbRetryLogEntry entry) {
		// If in a retry, then we have old records in the db
		byte[] value = transactionDbi.get(txn, key);
		if (value != null) {
			return RocksDbRetryLogEntry.fromTransactionValue(entry.getMessageId(), value);
		}
		return null;
	};

	private boolean storeDedup(RocksTransaction txn, byte[] transactionKey, RocksDbRetryLogEntry entry) {
		return duplicatesDbi.putIfAbsent(txn, transactionKey, entry.asDupEntry());// , PutFlags.MDB_NOOVERWRITE);
	}

	private boolean addRetryTimer(RocksTransaction txn, byte[] transactionKey, RocksDbRetryLogEntry entry,
			RocksDbRetryLogEntry previous) {
		if (previous != null) {
			timerDbi.delete(txn, previous.getTimerKey(), previous.asTimerValue());
		}

		timerDbi.put(txn, entry.getTimerKey(), entry.asTimerValue());// , PutFlags.MDB_NODUPDATA);

		return true;
	}

//	private Env<ByteBuffer> createEnv(final File path) {
//		return Env.create().setMapSize(32 * GB)//
//				.setMaxDbs(10)//
//				.setMaxReaders(4)//
//				.open(path, EnvFlags.MDB_NOSUBDIR, EnvFlags.MDB_MAPASYNC, EnvFlags.MDB_NOSYNC, EnvFlags.MDB_NORDAHEAD);
//		// EnvFlags.MDB_MAPASYNC, EnvFlags.MDB_NOSYNC,
//	}

//	private final static ByteBuffer longBb(long k) {
//		ByteBuffer value = ByteBuffer.allocateDirect(Long.BYTES);
//		value.putLong(k);
//		value.flip();
//		return value;
//	}

	/********************* STORAGE *******************/

	@Override
	public SnapshotInfo getLatestSnapshot() {
		return currentSnapshot;
	}

	@Override
	public void format() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy) throws IOException {
		helper.cleanupOldSnapshots(snapshotRetentionPolicy);
	}

	@Override
	public long takeSnapshot() {
		// get the last applied index
		final TermIndex last = this.getSnapshotInfo(); // this.supplier.getLastAppliedTermIndex();
		return takeSnapshot(last);
	}

	@Override
	public void pause() {
		//this.termIndex = this.getSnapshotInfo();
		//this.close();
	}

	private long takeSnapshot(final TermIndex last) {
		// create a file with a proper name to store the snapshot
		final File snapshotFile = helper.getSnapshotFile(last.getTerm(), last.getIndex());
		final String checkpointDir = helper.getCheckpointDir(last.getTerm(), last.getIndex());
		
		// create snapshot of the backend
		try (AutoCloseableLock l = lock.writeLock()) {
			storeSnapshotInfo(last);

			env.checkpoint(checkpointDir, snapshotFile).thenApply((a) -> {
				try {
					MD5Hash fileDigest = MD5FileUtil.computeMd5ForFile(snapshotFile);
					MD5FileUtil.saveMD5File(snapshotFile, fileDigest);
					final FileInfo fileInfo = new FileInfo(snapshotFile.toPath(), fileDigest);
					currentSnapshot = new SingleFileSnapshotInfo(fileInfo, last.getTerm(), last.getIndex());
				} catch (IOException e) {
					LOG.error("Error taking snapshot... to {}", snapshotFile, e);
				}
				return last;
			});
		}

		return last.getIndex();
	}
}