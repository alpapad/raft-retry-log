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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aktarma.retrylog.common.wire.proto.Request;

public interface RetryLogCommon {
	Logger LOG = LoggerFactory.getLogger(RetryLogCommon.class);

	public static Request unwrap(RaftProtos.LogEntryProto entry) {
		try {
			return Request.parseFrom(entry.getStateMachineLogEntry().getLogData());
		} catch (InvalidProtocolBufferException e) {
			LOG.error("Error unmarshalling entry {}", entry.getIndex(), e);
		}
		return null;
	}



	static <T> CompletableFuture<T> completeExceptionally(long index, String message) {
		return completeExceptionally(index, message, null);
	}

	static <T> CompletableFuture<T> completeExceptionally(long index, String message, Throwable cause) {
		return completeExceptionally(message + ", index=" + index, cause);
	}

	static <T> CompletableFuture<T> completeExceptionally(String message, Throwable cause) {
		return JavaUtils.completeExceptionally(new IOException(message).initCause(cause));
	}
}
