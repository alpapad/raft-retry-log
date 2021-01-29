package com.aktarma.retrylog.server;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.metrics.JVMMetrics;
import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.TimeDuration;

/**
 * Run with: -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:ParallelGCThreads=20 -XX:ConcGCThreads=5 -XX:InitiatingHeapOccupancyPercent=70
 */
public final class RetrylogServer {

  private RetrylogServer(){
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length < 1) {
      System.err.println("Usage: java -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:ParallelGCThreads=20 -XX:ConcGCThreads=5 -XX:InitiatingHeapOccupancyPercent=70 -cp *.jar com.aktarma.retrylog.server.RetrylogServer {serverIndex}");
      System.err.println("{serverIndex} could be 1, 2 or 3");
      System.exit(1);
    }

    //find current peer object based on application parameter
    RaftPeer currentPeer =
        ClusterCommon.PEERS.get(Integer.parseInt(args[0]) - 1);

    //create a property object
    RaftProperties raftProperties = new RaftProperties();

    //set the storage directory (different for each peer) in RaftProperty object
    File raftStorageDir = new File("/home/tests/" + currentPeer.getId().toString());
    RaftServerConfigKeys.setStorageDir(raftProperties, Collections.singletonList(raftStorageDir));
    
//	int raftSegmentPreallocatedSize = 1024 * 1024 * 1024;

	RaftConfigKeys.Rpc.setType(raftProperties, SupportedRpcType.GRPC);
	RaftServerConfigKeys.Rpc.setRequestTimeout(raftProperties, TimeDuration.valueOf(10, TimeUnit.SECONDS));
//	GrpcConfigKeys.setMessageSizeMax(raftProperties, SizeInBytes.valueOf(raftSegmentPreallocatedSize));
//	RaftServerConfigKeys.Log.Appender.setBufferByteLimit(raftProperties, SizeInBytes.valueOf(raftSegmentPreallocatedSize));
//	RaftServerConfigKeys.Log.setWriteBufferSize(raftProperties, SizeInBytes.valueOf(raftSegmentPreallocatedSize));
//	RaftServerConfigKeys.Log.setPreallocatedSize(raftProperties, SizeInBytes.valueOf(raftSegmentPreallocatedSize));
//	RaftServerConfigKeys.Log.setSegmentSizeMax(raftProperties, SizeInBytes.valueOf(1 * 1024 * 1024 * 1024L));
	    
    //RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.NETTY);
//    RaftServerConfigKeys.Log.setQueueElementLimit(properties, 81920);
    RaftServerConfigKeys.Log.setForceSyncNum(raftProperties, 512);

    RaftServerConfigKeys.Log.StateMachineData.setCachingEnabled(raftProperties, false);
    RaftServerConfigKeys.Log.StateMachineData.setSync(raftProperties, true);
    //RaftServerConfigKeys.Log.StateMachineData.setSyncTimeout(raftProperties,);
    RaftServerConfigKeys.Log.StateMachineData.setSyncTimeoutRetry(raftProperties, 3);
    
    
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(raftProperties, true);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(raftProperties, 1_000_000);
    RaftServerConfigKeys.Snapshot.setRetentionFileNum(raftProperties, 4);
    
    
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(raftProperties, true);
    //RaftServerConfigKeys.Log.Appender.setSnapshotChunkSizeMax(raftProperties, SizeInBytes.valueOf("4MB"));

//    RaftServerConfigKeys.setStagingCatchupGap(properties, 100_000);
    
    
    //set the port which server listen to in RaftProperty object
    final int port = NetUtils.createSocketAddr(currentPeer.getAddress()).getPort();
    GrpcConfigKeys.Server.setPort(raftProperties, port);
    
    //NettyConfigKeys.Server.setPort(properties, port);

    //create the counter state machine which hold the counter value
    RetryLogStateMachine retryLogStateMachine = new RetryLogStateMachine();

    JVMMetrics.initJvmMetrics(TimeDuration.valueOf(1, TimeUnit.MINUTES));
    
    MetricRegistries.global().enableConsoleReporter(TimeDuration.ONE_MINUTE);
    
    //create and start the Raft server
    RaftServer server = RaftServer.newBuilder()
  
        .setGroup(ClusterCommon.RAFT_GROUP)
        .setProperties(raftProperties)
        .setServerId(currentPeer.getId())
        .setStateMachine(retryLogStateMachine)
        .build();
    server.start();

    Thread.sleep(1000000);
    while(!server.getLifeCycleState().equals(LifeCycle.State.CLOSED)) {
    	Thread.sleep(1000);
    }
    System.err.println(server.getLifeCycleState());
//    //exit when any input entered
//    Scanner scanner = new Scanner(System.in);
//    scanner.nextLine();
    server.close();
  }
}
