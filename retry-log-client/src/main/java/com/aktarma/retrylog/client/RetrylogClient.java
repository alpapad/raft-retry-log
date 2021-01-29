package com.aktarma.retrylog.client;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;

import com.aktarma.retrylog.common.wire.proto.OperationResultCode;
import com.aktarma.retrylog.common.wire.proto.Request;
import com.aktarma.retrylog.common.wire.proto.Response;
import com.aktarma.retrylog.common.wire.proto.TransmissionBeginRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionBeginResponse.Code;
import com.aktarma.retrylog.common.wire.proto.TransmissionCommitRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionConfirmedRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionFailRequest;

import com.aktarma.retrylog.server.ClusterCommon;

public class RetrylogClient {

	private RaftClient client = buildClient();

	public BeginCode begin(String networkRef, int retry, long beginPhaseTimeout, long expires) throws IOException {
		long ref = Instant.now().getEpochSecond();

		Request request = Request.newBuilder().setTransmissionBegin(TransmissionBeginRequest.newBuilder()//
				.setNetworkref(ByteString.copyFromUtf8(networkRef))//
				.setReftime(ref)//
				.setBeginPhaseExpires(ref + beginPhaseTimeout)//
				.setExpires(ref + expires)//
				.setRetry(retry)//
		).build();

		RaftClientReply reply = client.io().send(Message.valueOf(request.toByteString()));

		watch(reply.getLogIndex());
		Response r = Response.parseFrom(reply.getMessage().getContent());
		// TransmissionEntry returnedEntry =
		// r.getTransmissionBeginResponse().getEntry();
		Code code = r.getTransmissionBeginResponse().getCode();

		return BeginCode.valueOf(code.name());
	}

	public ResultCode commit(String networkRef, int retry, long expires) throws IOException {
		final long ref = Instant.now().getEpochSecond();
		
		Request request = Request.newBuilder().setTransmissionCommit(TransmissionCommitRequest.newBuilder()//
				.setNetworkref(ByteString.copyFromUtf8(networkRef))//
				.setReftime(ref)//
				.setExpires(ref + expires)//
				.setRetry(retry)//
		).build();

		RaftClientReply reply = client.io().send(Message.valueOf(request.toByteString()));

		watch(reply.getLogIndex());
		Response r = Response.parseFrom(reply.getMessage().getContent());
		// TransmissionEntry returnedEntry =
		// r.getTransmissionCommitResponse().getEntry();
		OperationResultCode code = r.getTransmissionCommitResponse().getCode();

		return ResultCode.valueOf(code.name());
	}

	public ResultCode fail(String networkRef, int retry) throws IOException {
		final long ref = Instant.now().getEpochSecond();
		
		Request request = Request.newBuilder().setTransmissionFail(TransmissionFailRequest.newBuilder()//
				.setNetworkref(ByteString.copyFromUtf8(networkRef))//
				.setReftime(ref)//
				.setRetry(retry)//
		).build();

		RaftClientReply reply = client.io().send(Message.valueOf(request.toByteString()));

		watch(reply.getLogIndex());
		Response r = Response.parseFrom(reply.getMessage().getContent());
		// TransmissionEntry returnedEntry =
		// r.getTransmissionCommitResponse().getEntry();
		OperationResultCode code = r.getTransmissionFailResponse().getCode();

		return ResultCode.valueOf(code.name());
	}

	public ResultCode confirm(String networkRef) throws IOException {
		Request request = Request.newBuilder().setTransmissionConfirmed(TransmissionConfirmedRequest.newBuilder()//
				.setNetworkref(ByteString.copyFromUtf8(networkRef))//
				.setReftime(Instant.now().getEpochSecond())//
		).build();

		RaftClientReply reply = client.io().send(Message.valueOf(request.toByteString()));

		watch(reply.getLogIndex());
		Response r = Response.parseFrom(reply.getMessage().getContent());
		// TransmissionEntry returnedEntry =
		// r.getTransmissionCommitResponse().getEntry();
		OperationResultCode code = r.getTransmissionConfirmedResponse().getCode();

		return ResultCode.valueOf(code.name());

	}

	public void getEntriesToRetry() {

	}

	void watch(long index) {
		try {
			/* RaftClientReply watchReply = */ client.async().watch(index, ReplicationLevel.ALL_COMMITTED).get();
//			
//		      watchReply.getCommitInfos().forEach(
//		              val -> System.err.println(val.getServer() + " " + (val.getCommitIndex() >= index)));
		} catch (UnsupportedOperationException e) {
			// the cluster does not support watch
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * build the RaftClient instance which is used to communicate to Counter cluster
	 *
	 * @return the created client of Counter cluster
	 */
	private static RaftClient buildClient() {
		RaftProperties raftProperties = new RaftProperties();
		// RaftConfigKeys.Rpc.setType(raftProperties, SupportedRpcType.NETTY);
		RaftClientConfigKeys.Rpc.setRequestTimeout(raftProperties, TimeDuration.valueOf(1500, TimeUnit.MILLISECONDS));
		RaftClientConfigKeys.Async.setOutstandingRequestsMax(raftProperties, 100);

		// NettyFactory f = new NettyFactory();

		RaftClient.Builder builder = RaftClient.newBuilder().setProperties(raftProperties)

				.setRaftGroup(ClusterCommon.RAFT_GROUP)
				.setClientRpc(new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), raftProperties));
		return builder.build();
	}

	public void close() throws IOException {
		client.close();
	}
}
