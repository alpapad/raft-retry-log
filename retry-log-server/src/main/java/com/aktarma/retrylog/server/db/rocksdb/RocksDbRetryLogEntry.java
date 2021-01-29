package com.aktarma.retrylog.server.db.rocksdb;

import java.nio.ByteBuffer;

import com.aktarma.retrylog.common.wire.proto.TransmissionBeginRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionCommitRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionConfirmedRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionFailRequest;

public class RocksDbRetryLogEntry {
	private final static int RETRY_HEADER_LEN = Integer.BYTES + Integer.BYTES;

	private final static int DBENTRY_HEADER_LEN = Long.BYTES + Integer.BYTES;

	private long term;
	private long index;
	private final byte[] messageId;
	private long refTime;
	private long beginPhaseExpires = 0;
	private final long expires;

	private final int retry;

	public static RocksDbRetryLogEntry from(long term, long index, TransmissionBeginRequest br) {
		return new RocksDbRetryLogEntry(term, index, br);
	}

	private RocksDbRetryLogEntry(long term, long index, TransmissionBeginRequest br) {
		this(term, index, br.getNetworkref().toByteArray(), br.getReftime(), br.getBeginPhaseExpires(), br.getExpires(),
				br.getRetry());
	}

	public static RocksDbRetryLogEntry from(long term, long index, TransmissionCommitRequest br) {
		return new RocksDbRetryLogEntry(term, index, br);
	}

	private RocksDbRetryLogEntry(long term, long index, TransmissionCommitRequest br) {
		this(term, index, br.getNetworkref().toByteArray(), br.getReftime(), 0, br.getExpires(), br.getRetry());
	}

	public static RocksDbRetryLogEntry from(long term, long index, TransmissionFailRequest br) {
		return new RocksDbRetryLogEntry(term, index, br);
	}

	private RocksDbRetryLogEntry(long term, long index, TransmissionFailRequest br) {
		this(term, index, br.getNetworkref().toByteArray(), br.getReftime(), 0, 0, br.getRetry());
	}

	public static RocksDbRetryLogEntry from(long term, long index, TransmissionConfirmedRequest br) {
		return new RocksDbRetryLogEntry(term, index, br);
	}

	private RocksDbRetryLogEntry(long term, long index, TransmissionConfirmedRequest br) {
		this(term, index, br.getNetworkref().toByteArray(), br.getReftime(), 0, 0, 0);
	}

	public RocksDbRetryLogEntry(long term, long index, byte[] messageId, long refTime, long beginPhaseExpires,
			long expires, int retry) {
		super();
		this.term = term;
		this.index = index;
		this.refTime = refTime;
		this.messageId = messageId;
		this.expires = expires;
		this.beginPhaseExpires = beginPhaseExpires;
		this.retry = retry;
	}

	public RocksDbRetryLogEntry(long term, long index, byte[] messageId, long refTime, long expires) {
		this(term, index, messageId, refTime, 0, expires, 0);
	}

	public RocksDbRetryLogEntry(long term, long index, byte[] inputIn) {
		super();
		this.term = term;
		this.index = index;
		ByteBuffer input = ByteBuffer.wrap(inputIn);
		this.expires = 0;
		this.refTime = 0;
		this.beginPhaseExpires = 0;

		this.retry = input.getInt();
		int msgLen = input.getInt();
		this.messageId = new byte[msgLen];
		input.get(messageId);
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

	public byte[] asTimerValue() {
		return asTimerValue(retry, messageId);
	}

	public static byte[] asTimerValue(int retry, byte[] messageId) {
		ByteBuffer value = ByteBuffer.allocate(RETRY_HEADER_LEN + messageId.length);
		value.putInt(retry);
		value.putInt(messageId.length);
		value.put(messageId);
		value.flip();
		return value.array();
	}

	public byte[] asDupEntry() {
		ByteBuffer value = ByteBuffer.allocate(DBENTRY_HEADER_LEN + messageId.length);
		value.putLong(expires);
		value.putInt(messageId.length);
		value.put(messageId);
		value.flip();
		return value.array();
	}

	
	public byte[] getTimerKey() {
		ByteBuffer value = ByteBuffer.allocate(Long.BYTES + Long.BYTES); //+ Long.BYTES 
		value.putLong(expires);
		value.putLong(index);
		value.flip();
		return value.array();
	}

	public byte[] getTransactionKey() {
		return messageId;
	}

	public byte[] asTransactionValue() {
		ByteBuffer value = ByteBuffer.allocate(Long.BYTES + Long.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES);
		value.putLong(term);
		value.putLong(index);
		value.putLong(beginPhaseExpires);
		value.putLong(expires);
		value.putInt(retry);
		value.flip();
		return value.array();
	}

	public static RocksDbRetryLogEntry fromTransactionValue(byte[] messageId, byte[] bufferIn) {
		return new RocksDbRetryLogEntry(messageId, bufferIn);
	}
	
	private RocksDbRetryLogEntry(byte[] messageId, byte[] bufferIn) {
		super();
		ByteBuffer buffer = ByteBuffer.wrap(bufferIn);
		
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
