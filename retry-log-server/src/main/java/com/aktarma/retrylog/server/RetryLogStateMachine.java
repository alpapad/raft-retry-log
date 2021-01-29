package com.aktarma.retrylog.server;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import com.aktarma.retrylog.common.wire.proto.ExceptionResponse;
import com.aktarma.retrylog.common.wire.proto.OperationResultCode;
import com.aktarma.retrylog.common.wire.proto.Request;
import com.aktarma.retrylog.common.wire.proto.Response;
import com.aktarma.retrylog.common.wire.proto.ToRetryRequest;
import com.aktarma.retrylog.common.wire.proto.ToRetryResponse;
import com.aktarma.retrylog.common.wire.proto.TransmissionBeginRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionBeginResponse;
import com.aktarma.retrylog.common.wire.proto.TransmissionCommitRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionCommitResponse;
import com.aktarma.retrylog.common.wire.proto.TransmissionConfirmedRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionConfirmedResponse;
import com.aktarma.retrylog.common.wire.proto.TransmissionEntry;
import com.aktarma.retrylog.common.wire.proto.TransmissionFailRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionFailResponse;
import com.aktarma.retrylog.common.wire.proto.TransmissionRetryEntry;
import com.aktarma.retrylog.server.db.DbBackendFactory;
import com.aktarma.retrylog.server.db.IDbBackend;
import com.aktarma.retrylog.server.db.OpResult;
import com.aktarma.retrylog.server.db.TermIndexSupplier;

public class RetryLogStateMachine extends BaseStateMachine implements TermIndexSupplier {

	private IDbBackend backend = null;

	private RaftStorage raftStorage;

	public RetryLogStateMachine() {
		super();
		backend = DbBackendFactory.getDb(this);
	}

	@Override
	public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(RaftProtos.RoleInfoProto roleInfoProto,
			TermIndex termIndex) {
		backend.reinitialize();
		this.setLastAppliedTermIndex(backend.getSnapshotInfo());
		return CompletableFuture.completedFuture(backend.getSnapshotInfo());
	}

	@Override
	public void pause() {
		backend.pause();
	}

	@Override
	public StateMachineStorage getStateMachineStorage() {
		return backend;
	}

	/**
	 * @param server      the current server information
	 * @param groupId     the cluster groupId
	 * @param raftStorage the raft storage which is used to keep raft related stuff
	 * @throws IOException if any error happens during load state
	 */
	@Override
	public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
		super.initialize(server, groupId, raftStorage);
		this.raftStorage = raftStorage;
		backend.init(this.raftStorage);
		this.setLastAppliedTermIndex(backend.getSnapshotInfo());
	}

	/**
	 * very similar to initialize method, but doesn't initialize the storage system
	 * because the state machine reinitialized from the PAUSE state and storage
	 * system initialized before.
	 *
	 * @throws IOException if any error happens during load state
	 */
	@Override
	public void reinitialize() throws IOException {
		backend.reinitialize();
		this.setLastAppliedTermIndex(backend.getSnapshotInfo());
	}

	/**
	 * @return the index of the snapshot
	 */
	@Override
	public long takeSnapshot() {
		return backend.takeSnapshot();
	}

//	@Override
//	public void notifyTermIndexUpdated(long term, long index) {
//		super.notifyTermIndexUpdated(term, index);
//		// CompletableFuture.supplyAsync(()-> backend.sync());
//	}

//	@Override
//	public TransactionContext applyTransactionSerial(TransactionContext trx) {
//		final Request request = RetryLogCommon.unwrap(trx.getLogEntry());
//		if (request != null && request.getRequestTypeCase().equals(Request.RequestTypeCase.TRANSMISSION_BEGIN)) {
//			return begin(trx, request);
//		}
//		return trx;
//	}
//
//	private TransactionContext begin(TransactionContext trx, Request request) {
//		final RaftProtos.LogEntryProto entry = Objects.requireNonNull(trx.getLogEntry());
//		final long index = entry.getIndex();
//		final long term = entry.getTerm();
//
//		TransmissionBeginRequest br = request.getTransmissionBegin();
//		trx.setStateMachineContext(backend.begin(term, index, br));
//		return trx;
//	}

//	public CompletableFuture<Void> flush(long logIndex) {
//		// LOG.error("Flushing up to {}" , logIndex);
//		// CompletableFuture.supplyAsync(() -> backend.sync());
//		// return CompletableFuture.allOf(CompletableFuture.supplyAsync(() ->
//		// backend.sync()), super.flush(logIndex));
//		return super.flush(logIndex);
//	}

	@Override
	public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
		final RaftProtos.LogEntryProto entry = Objects.requireNonNull(trx.getLogEntry());

		final long index = entry.getIndex();
		final long term = entry.getTerm();

		updateLastAppliedTermIndex(term, index);

		final Request request = RetryLogCommon.unwrap(entry);

		if (request == null) {
			return RetryLogCommon.completeExceptionally(index, "Could not unmashal request");
		}

		switch (request.getRequestTypeCase()) {
		case TRANSMISSION_BEGIN:
			return begin(trx, term, index, request.getTransmissionBegin());

		case RETRY_REQ:
			return retry(trx, request.getRetryReq());

		case TRANSMISSION_COMMIT:
			return commit(trx, term, index, request.getTransmissionCommit());

		case TRANSMISSION_FAIL:
			return fail(trx, term, index, request.getTransmissionFail());

		case TRANSMISSION_CONFIRMED:
			return confirm(trx, term, index, request.getTransmissionConfirmed());

		case REQUESTTYPE_NOT_SET:
			break;
		default:
			break;
		}
		return RetryLogCommon.completeExceptionally(index, "Message received is not handled by this state machine");
	}

	private CompletableFuture<Message> begin(TransactionContext trx, long term, long index,
			TransmissionBeginRequest request) {
		return sync(trx, builder -> {
			TransmissionBeginResponse.Code code = backend.begin(term, index, request);

			// TransmissionBeginResponse.Code code = TransmissionBeginResponse.Code.class.cast(trx.getStateMachineContext());

			builder.setTransmissionBeginResponse(TransmissionBeginResponse.newBuilder()//
					.setEntry(TransmissionEntry.newBuilder()//
							.setNetworkref(request.getNetworkref())//
							.setRetry(request.getRetry()))
					.setCode(code));
		});
	}

	private CompletableFuture<Message> fail(TransactionContext trx, long term, long index,
			TransmissionFailRequest request) {

		return sync(trx, builder -> {
			OpResult result = this.backend.fail(term, index, request);

			builder.setTransmissionFailResponse(TransmissionFailResponse.newBuilder()//
					.setEntry(result.getEntry())//
					.setCode(toCode(result)));
		});
	}

	private CompletableFuture<Message> confirm(TransactionContext trx, long term, long index,
			TransmissionConfirmedRequest request) {

		return sync(trx, builder -> {
			OpResult result = this.backend.confirm(term, index, request);

			builder.setTransmissionConfirmedResponse(TransmissionConfirmedResponse.newBuilder()//
					.setEntry(result.getEntry())//
					.setCode(toCode(result)));
		});

	}

	private CompletableFuture<Message> commit(TransactionContext trx, long term, long index,
			TransmissionCommitRequest request) {

		return sync(trx, builder -> {
			OpResult result = this.backend.commit(term, index, request);

			builder.setTransmissionCommitResponse(TransmissionCommitResponse.newBuilder()//
					.setEntry(result.getEntry())//
					.setCode(toCode(result)));
		});
	}

	private CompletableFuture<Message> retry(TransactionContext trx, ToRetryRequest retryReq) {
		return null;
	}

	private static CompletableFuture<Message> sync(TransactionContext trx, Consumer<Response.Builder> c) {
		return CompletableFuture.completedFuture(respond(trx, c));
	}

//	private static CompletableFuture<Message> async(TransactionContext trx, Consumer<Response.Builder> c) {
//		return CompletableFuture.supplyAsync(() -> respond(trx, c));
//
//	}

//	private  void updateLastAppliedTermIndex(TransactionContext trx) {
//		final RaftProtos.LogEntryProto entry = trx.getLogEntry();
//		updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
//	}

	private static Message respond(TransactionContext trx, Consumer<Response.Builder> c) {
		Response.Builder builder = Response.newBuilder();
		try {
			c.accept(builder);
		} catch (Exception e) {
			LOG.error("Error:", e);
			return Message.valueOf(builder.setException(toExceptionResponse(e)).build().toByteString(),
					() -> "Message:" + e.getMessage());
		}
		return Message.valueOf(builder.build().toByteString());
	}

	private static OperationResultCode toCode(OpResult result) {
		return OperationResultCode.valueOf(result.getCode().name());
	}

	/**
	 * Return retryable references
	 *
	 * @param request the GET message
	 * @return the Message containing the current counter value
	 */
	@Override
	public CompletableFuture<Message> query(Message request) {
		ToRetryRequest rq;
		try {
			rq = ToRetryRequest.parseFrom(request.getContent().toByteArray());
		} catch (InvalidProtocolBufferException e) {
			ByteString buf = ToRetryResponse.newBuilder()
					.setException(ExceptionResponse.newBuilder().setExceptionClassName("me").build()).build()
					.toByteString();
			return CompletableFuture.completedFuture(Message.valueOf(buf, () -> "Message:" + e));
		}
		LOG.info("Query request from {} ", rq.getRequestor().toString());

		ToRetryResponse.Builder builder = ToRetryResponse.newBuilder();

		for (int i = 0; i < 10; i++) {
			builder.addEntry(TransmissionRetryEntry.newBuilder().setNetworkref(ByteString.copyFromUtf8("a" + i))
					.setRetry(1).build());
		}
		return CompletableFuture.completedFuture(Message.valueOf(builder.build().toByteString(), () -> "Message:"));
	}

	private static ExceptionResponse toExceptionResponse(Exception e) {
		return ExceptionResponse.newBuilder().setExceptionClassName(e.getClass().getName())
				.setStackTrace(ExceptionUtils.getStackTrace(e)).setMessage(e.getMessage()).build();
	}

//	@Override
//	public TermIndex getLastAppliedTermIndex() {
//		return this.getLastAppliedTermIndex();
//	}
}
