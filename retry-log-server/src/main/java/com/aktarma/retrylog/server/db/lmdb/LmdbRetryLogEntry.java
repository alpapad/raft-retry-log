package com.aktarma.retrylog.server.db.lmdb;

import java.nio.ByteBuffer;

import com.aktarma.retrylog.common.wire.proto.TransmissionBeginRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionCommitRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionConfirmedRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionFailRequest;

public class LmdbRetryLogEntry {
	private final static int RETRY_HEADER_LEN = Integer.BYTES + Integer.BYTES;

	private final static int DBENTRY_HEADER_LEN = Long.BYTES + Integer.BYTES;

	private final long term;
	private final long index;
	private final byte[] messageId;
	private long refTime;
	private long beginPhaseExpires = 0;
	private final long expires;

	private final int retry;

	public static LmdbRetryLogEntry from(long term, long index, TransmissionBeginRequest br) {
		return new LmdbRetryLogEntry(term, index, br);
	}

	private LmdbRetryLogEntry(long term, long index, TransmissionBeginRequest br) {
		this(term, index, br.getNetworkref().toByteArray(), br.getReftime(),
				br.getBeginPhaseExpires(), br.getExpires(), br.getRetry());
	}

	public static LmdbRetryLogEntry from(long term, long index, TransmissionCommitRequest br) {
		return new LmdbRetryLogEntry(term, index, br);
	}
	
	private LmdbRetryLogEntry(long term, long index, TransmissionCommitRequest br) {
		this(term, index, br.getNetworkref().toByteArray(), br.getReftime(), 0, br.getExpires(), br.getRetry());
	}

	public static LmdbRetryLogEntry from(long term, long index, TransmissionFailRequest br) {
		return new LmdbRetryLogEntry(term, index, br);
	}

	private LmdbRetryLogEntry(long term, long index, TransmissionFailRequest br) {
		this(term, index, br.getNetworkref().toByteArray(), br.getReftime(), 0, 0, br.getRetry());
	}

	public static LmdbRetryLogEntry from(long term, long index, TransmissionConfirmedRequest br) {
		return new LmdbRetryLogEntry(term, index, br);
	}
	
	private LmdbRetryLogEntry(long term, long index, TransmissionConfirmedRequest br) {
		this(term, index, br.getNetworkref().toByteArray(), br.getReftime(), 0, 0, 0);
	}

	public LmdbRetryLogEntry(long term, long index, byte[] messageId, long refTime, long beginPhaseExpires, long expires, int retry) {
		super();
		this.term = term;
		this.index=index;
		this.refTime = refTime;
		this.messageId = messageId;
		this.expires = expires;
		this.beginPhaseExpires = beginPhaseExpires;
		this.retry = retry;
	}

	public LmdbRetryLogEntry(long term, long index, byte[] messageId, long refTime, long expires) {
		this(term, index, messageId, refTime, 0, expires, 0);
	}

	public byte[] getMessageId() {
		return messageId;
	}

	public long getExpires() {
		return expires;
	}

	public int getRetry() {
		return retry;
	}

	public long getRefTime() {
		return refTime;
	}

	public void setRefTime(long refTime) {
		this.refTime = refTime;
	}

	public long getBeginPhaseExpires() {
		return beginPhaseExpires;
	}

	public void setBeginPhaseExpires(long beginPhaseExpires) {
		this.beginPhaseExpires = beginPhaseExpires;
	}

	public ByteBuffer asTimerValue() {
		ByteBuffer value = ByteBuffer.allocateDirect(RETRY_HEADER_LEN + messageId.length);
		value.putInt(retry);
		value.putInt(messageId.length);
		value.put(messageId);
		value.flip();
		return value;
	}

	public ByteBuffer asDupEntry() {
		ByteBuffer value = ByteBuffer.allocateDirect(DBENTRY_HEADER_LEN + messageId.length);
		value.putLong(expires);
		value.putInt(messageId.length);
		value.put(messageId);
		value.flip();
		return value;
	}

	private final static int TIMER_KEY_BSIZE = Long.BYTES  + Long.BYTES;
	
	public ByteBuffer getTimerKey() {
		ByteBuffer value = ByteBuffer.allocateDirect(TIMER_KEY_BSIZE);
		value.putLong(expires);
		value.putLong(index);
		value.flip();
		return value;
	}

	public ByteBuffer getTransactionKey() {
		ByteBuffer buffer = ByteBuffer.allocateDirect(messageId.length);
		buffer.put(messageId);
		buffer.flip();
		return buffer;
	}

	private final static int TRANSACTION_VALUE_BSIZE = Long.BYTES + Long.BYTES +Long.BYTES + Long.BYTES + Integer.BYTES;
	
	public ByteBuffer asTransactionValue() {
		ByteBuffer value = ByteBuffer.allocateDirect(TRANSACTION_VALUE_BSIZE);
		value.putLong(term);
		value.putLong(index);
		value.putLong(beginPhaseExpires);
		value.putLong(expires);
		value.putInt(retry);
		value.flip();
		return value;
	}
	
	public static LmdbRetryLogEntry fromTransactionValue(byte[] messageId, ByteBuffer buffer) {
		return new LmdbRetryLogEntry(messageId, buffer);
	}
	
	private LmdbRetryLogEntry(byte[] messageId, ByteBuffer buffer) {
		super();
		this.refTime = 0;
		this.messageId = messageId;
		this.term = buffer.getLong();
		this.index = buffer.getLong();
		this.beginPhaseExpires = buffer.getLong();
		this.expires = buffer.getLong();
		this.retry = buffer.getInt();
	}

	@Override
	public String toString() {
		return "RetryLogEntry [messageId=" + (messageId != null ? new String(messageId) : "") + ", refTime=" + refTime
				+ ", beginPhaseExpires=" + beginPhaseExpires + ", expires=" + expires + ", retry=" + retry + "]";
	}

}
