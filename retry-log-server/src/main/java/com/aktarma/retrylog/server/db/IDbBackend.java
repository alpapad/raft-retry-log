package com.aktarma.retrylog.server.db;

import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.StateMachineStorage;

import com.aktarma.retrylog.common.wire.proto.TransmissionBeginRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionBeginResponse.Code;
import com.aktarma.retrylog.common.wire.proto.TransmissionCommitRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionConfirmedRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionFailRequest;

public interface IDbBackend extends StateMachineStorage{

	boolean sync();

	void info();
	
	Code begin(long term, long index, TransmissionBeginRequest request);

	OpResult commit(long term, long index, TransmissionCommitRequest request);

	OpResult fail(long term, long index, TransmissionFailRequest request);

	OpResult confirm(long term, long index, TransmissionConfirmedRequest request);

	//OpResult discard(TransmissionDiscardRequest RetryLogEntry entry);

	// snapshot related
	void reinitialize();

	long takeSnapshot();
	
	TermIndex getSnapshotInfo();

	void close();

	void pause();
}