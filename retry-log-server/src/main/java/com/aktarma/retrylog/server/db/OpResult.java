package com.aktarma.retrylog.server.db;

import com.aktarma.retrylog.common.wire.proto.TransmissionEntry;
public class OpResult {
	private final TransmissionEntry entry;
	private final OperationResultCode code;

	public OpResult(TransmissionEntry entry, OperationResultCode code) {
		super();
		this.entry = entry;
		this.code = code;
	}

	public TransmissionEntry getEntry() {
		return entry;
	}

	public OperationResultCode getCode() {
		return code;
	}
}
