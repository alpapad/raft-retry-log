package com.aktarma.retrylog.server.db;

public class StorageException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public StorageException(Exception e) {
		super(e);
	}

}
