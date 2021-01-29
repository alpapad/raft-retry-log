package com.aktarma.retrylog.server.db.lmdb;

import static org.lmdbjava.DbiFlags.MDB_CREATE;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.MD5FileUtil;
import org.lmdbjava.CopyFlags;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.PutFlags;
import org.lmdbjava.Txn;
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

/**
 * LMDB transactions use serializable isolation level. This means only one
 * writer is active at any time
 */
public class LmdbDbBackend implements IDbBackend {
	private static final Logger LOG = LoggerFactory.getLogger(LmdbDbBackend.class);

	private final static String TRANSACTION_DB = "transaction";
	private final static String DUP_DB = "dupdb";
	private final static String TIMER_DB = "timer";
	private final static String SNAPSHOT_DB = "snapshot";

	private Env<ByteBuffer> env;

	private Dbi<ByteBuffer> duplicatesDbi;
	private Dbi<ByteBuffer> transactionDbi;
	private Dbi<ByteBuffer> timerDbi;
	private Dbi<ByteBuffer> snapshotDbi;
	private final RwLock lock = new RwLock();

	private final static long GB = 1073741824;

	private final static ByteBuffer TERM_KEY = longBb(0l);
	private final static ByteBuffer INDEX_KEY = longBb(1l);

	private final TermIndexSupplier supplier;

	private volatile SingleFileSnapshotInfo currentSnapshot = null;

	private RaftStorage raftStorage;
	private File smDir = null;
	private File lmdbDir = null;
	private File lmdbDbFile = null;
	private LmdbStorageHelper helper;
	
	private TermIndex termIndex = null;

	

	public LmdbDbBackend(TermIndexSupplier supplier) {
		super();
		this.supplier = supplier;
	}

	private final void startDb() {
		// should reload from start as a new snapshot might have arrived!
		this.currentSnapshot = helper.findLatestSnapshot();

		boolean shouldLoadFromSnapshot;
		if (currentSnapshot == null) {
			initialize();
			shouldLoadFromSnapshot = checkForNewerSnapshot(null);
		} else if (this.lmdbDbFile.exists()) {
			initialize();
			shouldLoadFromSnapshot = checkForNewerSnapshot(currentSnapshot.getTermIndex());
		} else {
			shouldLoadFromSnapshot = true;
		}

		if (shouldLoadFromSnapshot) {
			close();
			try {
				Files.copy(currentSnapshot.getFile().getPath(), this.lmdbDbFile.toPath(),
						StandardCopyOption.REPLACE_EXISTING);
			} catch (IOException e) {
				LOG.error("Error copying {} to {}, ", currentSnapshot.getFile().getPath(), this.lmdbDbFile.toPath(), e);
				throw new StorageException(e);
			}
			initialize();
		}
	}
	@Override
	public void init(RaftStorage raftStorage) throws IOException {
		this.helper = new LmdbStorageHelper(raftStorage.getStorageDir().getStateMachineDir());
		this.raftStorage = raftStorage;
		this.smDir = this.raftStorage.getStorageDir().getStateMachineDir();
		this.lmdbDir = new File(smDir, "lmdb/");
		this.lmdbDir.mkdirs();
		this.lmdbDbFile = new File(lmdbDir, "current.mdb");

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

		env = this.createEnv(lmdbDbFile);
		// keeps exactly one version of the message id (the latest)
		transactionDbi = env.openDbi(TRANSACTION_DB, MDB_CREATE);

		// keeps exactly one timer of the message id (the latest) but we might have
		// multiple message ids under the same key
		timerDbi = env.openDbi(TIMER_DB,  DbiFlags.MDB_CREATE);//, DbiFlags.MDB_DUPSORT, DbiFlags.MDB_INTEGERDUP);

		// keeps exactly one dedup instance
		duplicatesDbi = env.openDbi(DUP_DB, MDB_CREATE);
		snapshotDbi = env.openDbi(SNAPSHOT_DB, DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY);
		try (Txn<ByteBuffer> txn = env.txnWrite()) {
			if (null == snapshotDbi.get(txn, TERM_KEY)) {
				snapshotDbi.put(txn, TERM_KEY, longBb(0l));
			}

			if (null == snapshotDbi.get(txn, INDEX_KEY)) {
				snapshotDbi.put(txn, INDEX_KEY, longBb(RaftLog.INVALID_LOG_INDEX));
			}
			txn.commit();
		}
		// Live db provides termindex stored in db
		this.termIndex = null;
	}

	@Override
	public boolean sync() {
		storeSnapshotInfo(this.supplier.getLastAppliedTermIndex());
		env.sync(true);
		return true;
	}

	@Override
	public void info() {
		try (AutoCloseableLock l = lock.readLock(); Txn<ByteBuffer> txn = env.txnWrite()) {
			System.err.println("");
			System.err.println("\tTransactions : " + transactionDbi.stat(txn));
			System.err.println("\tTimers       : " + timerDbi.stat(txn));
			System.err.println("\tDuplicates   : " + duplicatesDbi.stat(txn));
			System.err.println("\tSnapshots    : " + snapshotDbi.stat(txn));
		}
		System.err.println("\tEnvironment  : " + env.stat());
	}

	@Override
	public TransmissionBeginResponse.Code begin(long term, long index, TransmissionBeginRequest request) {
		LmdbRetryLogEntry entry = LmdbRetryLogEntry.from(term, index, request);
		ByteBuffer transactionKey = entry.getTransactionKey();
		ByteBuffer transactionValue = entry.asTransactionValue();

		try (AutoCloseableLock l = lock.readLock(); Txn<ByteBuffer> txn = env.txnWrite()) {
			storeSnapshotInfo(txn, term, index);
			// If it is the first time we see this message, then we do a dedup and then we
			// try to insert.
			if (entry.getRetry() == 0) {
				// We assume the entry will not be there

				if (storeDedup(txn, transactionKey, entry)) {
					if (transactionDbi.put(txn, transactionKey, transactionValue, PutFlags.MDB_NOOVERWRITE)) {
						if(!addRetryTimer(txn, transactionKey, entry, null)) {
							LOG.error("Could not add message in timers log: {}", entry);
						}
						txn.commit();
						return Code.OK;
					} else {
						// SHOULD NOT HAPPEN
						txn.abort();
						return Code.INFLIGHT;
					}
				} else {
					txn.abort();
					return Code.DUPLICATE;
				}
			} else {
				// If in a retry, then we have old records in the db. Try to load it
				// MUST not be null..
				LmdbRetryLogEntry previous = loadPrevious(txn, transactionKey, entry);
				if (previous == null) {
					txn.abort();
					// Or maybe follow the 0 retry path?
					return Code.NOT_FOUND;
				}
				// if old value is stale, overwrite
				if (previous.getBeginPhaseExpires() < entry.getRefTime()) {
					// delete old version if it matches exactly (it should)
					// transactionDbi.delete(txn, key, previous.asMessageIdValue());

					if (transactionDbi.put(txn, transactionKey, transactionValue)) {
						if(!addRetryTimer(txn, transactionKey, entry, previous)) {
							LOG.error("Could not add message in timers log: {}", entry);
						}
						txn.commit();
						return Code.OK;
					}
				}
				LOG.debug("Message id {} exists, expires {} while ref-time is {} ", new String(entry.getMessageId()),
						previous.getBeginPhaseExpires(), entry.getRefTime());
				txn.abort();
				return Code.INFLIGHT;
			}
		}
	}

	@Override
	public OpResult commit(long term, long index, TransmissionCommitRequest request) {
		LmdbRetryLogEntry entry = LmdbRetryLogEntry.from(term, index, request);
		ByteBuffer key = entry.getTransactionKey();

		try (AutoCloseableLock l = lock.readLock(); Txn<ByteBuffer> txn = env.txnWrite()) {
			storeSnapshotInfo(txn, term, index);
			LmdbRetryLogEntry prev = loadPrevious(txn, key, entry);

			if (prev == null) {
				LOG.error("Error, we should have a previous value.. for {}", entry);
				txn.abort();
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
		}
	}

	@Override
	public OpResult fail(long term, long index, TransmissionFailRequest request) {
		LmdbRetryLogEntry entry = LmdbRetryLogEntry.from(term, index, request);

		ByteBuffer key = entry.getTransactionKey();

		try (AutoCloseableLock l = lock.readLock(); Txn<ByteBuffer> txn = env.txnWrite()) {
			storeSnapshotInfo(txn, term, index);
			LmdbRetryLogEntry prev = loadPrevious(txn, key, entry);
			if (prev == null) {
				LOG.error("Error, we should have a previous value.. for {}", entry);
				txn.abort();
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
		}
	}

	@Override
	public OpResult confirm(long term, long index, TransmissionConfirmedRequest request) {
		LmdbRetryLogEntry entry = LmdbRetryLogEntry.from(term, index, request);
		ByteBuffer key = entry.getTransactionKey();

		try (AutoCloseableLock l = lock.readLock(); Txn<ByteBuffer> txn = env.txnWrite()) {
			storeSnapshotInfo(txn, term, index);
			LmdbRetryLogEntry prev = loadPrevious(txn, key, entry);
			if (prev == null) {
				LOG.error("Confirm: we should have a previous value for {}. Assuming it was confirmed before?", entry);
				txn.abort();
				return toOpResult(prev, OperationResultCode.NOT_FOUND);
			}

			if (!transactionDbi.delete(txn, key)) {
				LOG.error("Could not remove prev transaction for :{}. Prev is: {}", entry, prev);
			}

			if (!timerDbi.delete(txn, prev.getTimerKey())) {
				LOG.error("Could not remove prev timer for :{}. Prev is:{}", entry, prev);
			}

			txn.commit();
			return toOpResult(prev, OperationResultCode.OK);
		}
	}

//	@Override
//	public OpResult discard(RetryLogEntry entry) {
//		ByteBuffer key = entry.getMsgIdKey();
//
//		try (AutoCloseableLock l = lock.readLock(); Txn<ByteBuffer> txn = env.txnWrite()) {
//			storeSnapshotInfo(txn);
//
//			RetryLogEntry prev = loadPrevious(txn, key, entry);
//			if (prev == null) {
//				LOG.error("Error, we should have a previous value.. for {}", entry);
//				txn.abort();
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
		if(termIndex != null) {
			return termIndex;
		}
		long term = 0;
		long index = RaftLog.INVALID_LOG_INDEX;
		try (AutoCloseableLock l = lock.readLock(); Txn<ByteBuffer> txn = env.txnRead()) {
			ByteBuffer bb = snapshotDbi.get(txn, TERM_KEY);
			term = bb.getLong();
			bb = snapshotDbi.get(txn, INDEX_KEY);
			index = bb.getLong();
		}
		return TermIndex.valueOf(term, index);
	}

	@Override
	public void close() {
		if (transactionDbi != null) {
			transactionDbi.close();
			transactionDbi = null;
		}
		if (timerDbi != null) {
			timerDbi.close();
			timerDbi = null;
		}
		if (duplicatesDbi != null) {
			duplicatesDbi.close();
			duplicatesDbi = null;
		}
		if (snapshotDbi != null) {
			snapshotDbi.close();
			snapshotDbi = null;
		}
		if (env != null) {
			env.close();
			env = null;
		}
	}

	private OpResult toOpResult(LmdbRetryLogEntry prev, OperationResultCode code) {
		return new OpResult(TransmissionEntry.newBuilder()//
				.setNetworkref(ByteString.copyFrom(prev.getMessageId()))//
				.setRetry(prev.getRetry()).build(), //
				code);
	}

	private void storeSnapshotInfo(Txn<ByteBuffer> txn, long term, long index) {
		snapshotDbi.put(txn, TERM_KEY, longBb(term));
		snapshotDbi.put(txn, INDEX_KEY, longBb(index));
	}

	private void storeSnapshotInfo(TermIndex idx) {
		try (Txn<ByteBuffer> txn = env.txnWrite()) {
			storeSnapshotInfo(txn, idx.getTerm(), idx.getIndex());
			txn.commit();
		}
	}

	private LmdbRetryLogEntry loadPrevious(Txn<ByteBuffer> txn, ByteBuffer key, LmdbRetryLogEntry entry) {
		// If in a retry, then we have old records in the db
		ByteBuffer value = transactionDbi.get(txn, key);
		if (value != null) {
			return LmdbRetryLogEntry.fromTransactionValue(entry.getMessageId(), value);
		}
		return null;
	};

	private boolean storeDedup(Txn<ByteBuffer> txn, ByteBuffer key, LmdbRetryLogEntry entry) {
		return duplicatesDbi.put(txn, key, entry.asDupEntry(), PutFlags.MDB_NOOVERWRITE);
	}

	private boolean addRetryTimer(Txn<ByteBuffer> txn, ByteBuffer key, LmdbRetryLogEntry entry,
			LmdbRetryLogEntry previous) {
		if (previous != null) {
			if(!timerDbi.delete(txn, previous.getTimerKey())) {
				LOG.error("Could not remove prev timer for :{}. Prev is:{}", entry, previous);
			}
		}

		return timerDbi.put(txn, entry.getTimerKey(), entry.asTimerValue(), PutFlags.MDB_NOOVERWRITE);
	}

	private Env<ByteBuffer> createEnv(final File path) {
		return Env.create().setMapSize(32 * GB)//
				.setMaxDbs(10)//
				.setMaxReaders(4)//
				.open(path, EnvFlags.MDB_NOSUBDIR, /*EnvFlags.MDB_MAPASYNC,*/ EnvFlags.MDB_NOSYNC, EnvFlags.MDB_NORDAHEAD);
		// EnvFlags.MDB_MAPASYNC, EnvFlags.MDB_NOSYNC,
	}

	private final static ByteBuffer longBb(long k) {
		ByteBuffer value = ByteBuffer.allocateDirect(Long.BYTES);
		value.putLong(k);
		value.flip();
		return value;
	}

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
	}
	
	private long takeSnapshot(final TermIndex last) {
		// create a file with a proper name to store the snapshot
		final File snapshotFile = helper.getSnapshotFile(last.getTerm(), last.getIndex());

		// crate snapshot of the backend
		try (AutoCloseableLock l = lock.writeLock()) {
			storeSnapshotInfo(last);
			env.sync(true);
			env.copy(snapshotFile, CopyFlags.MDB_CP_COMPACT);
		}

		try {

			MD5Hash fileDigest = MD5FileUtil.computeMd5ForFile(snapshotFile);
			MD5FileUtil.saveMD5File(snapshotFile, fileDigest);
			// MD5Hash fileDigest = MD5FileUtil.readStoredMd5ForFile(snapshotFile);
			final FileInfo fileInfo = new FileInfo(snapshotFile.toPath(), fileDigest);
			currentSnapshot = new SingleFileSnapshotInfo(fileInfo, last.getTerm(), last.getIndex());
		} catch (IOException e) {
			LOG.error("Error taking snapshot... to {}", snapshotFile, e);
		}

		// return the index of the stored snapshot (which is the last applied one)
		return last.getIndex();
	}



}
