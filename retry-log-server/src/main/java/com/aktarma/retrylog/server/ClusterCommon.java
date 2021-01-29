/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aktarma.retrylog.server;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

/**
 * Common Constant across servers and client
 */
public final class ClusterCommon {
  public static final List<RaftPeer> PEERS = new ArrayList<>(5);

  static {
	    PEERS.add(RaftPeer.newBuilder().setId("n1").setAddress("127.0.0.1:6000").setPriority(1).build());
	    PEERS.add(RaftPeer.newBuilder().setId("n2").setAddress("127.0.0.1:6001").setPriority(2).build());
	    PEERS.add(RaftPeer.newBuilder().setId("n3").setAddress("127.0.0.1:6002").setPriority(3).build());
//	    PEERS.add(RaftPeer.newBuilder().setId("n4").setAddress("127.0.0.1:6003").setPriority(4).build());
//	    PEERS.add(RaftPeer.newBuilder().setId("n5").setAddress("127.0.0.1:6004").setPriority(5).build());
  }

  private ClusterCommon() {
  }

  private static final UUID CLUSTER_GROUP_ID = UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1");
  public static final RaftGroup RAFT_GROUP = RaftGroup.valueOf(
      RaftGroupId.valueOf(ClusterCommon.CLUSTER_GROUP_ID), PEERS);
}
