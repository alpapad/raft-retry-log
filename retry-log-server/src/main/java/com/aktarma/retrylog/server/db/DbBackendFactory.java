package com.aktarma.retrylog.server.db;


public class DbBackendFactory {

	public static IDbBackend getDb(TermIndexSupplier tisup) {
		//return new com.aktarma.retrylog.server.db.rocksdb.RocksDbBackend(tisup);
		return  new com.aktarma.retrylog.server.db.lmdb.LmdbDbBackend(tisup);
	}
}
