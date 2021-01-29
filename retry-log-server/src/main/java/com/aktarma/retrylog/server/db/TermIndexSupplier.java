package com.aktarma.retrylog.server.db;

import org.apache.ratis.server.protocol.TermIndex;

@FunctionalInterface
public interface TermIndexSupplier {

	TermIndex getLastAppliedTermIndex();
}
