package com.aktarma.retrylog.server.db;

import java.io.File;
import java.io.IOException;

import org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageDirectory;
import org.apache.ratis.server.storage.RaftStorageMetadataFile;

public class TestRaftStorage implements RaftStorage {

	final RaftStorageDirectory dir;
	
	public TestRaftStorage(File root) {
		super();
		dir = new RaftStorageDirectory() {

			@Override
			public File getRoot() {
				return root;
			}

			@Override
			public boolean isHealthy() {
				return true;
			}
			
		};
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public RaftStorageDirectory getStorageDir() {
		return dir;
	}

	@Override
	public RaftStorageMetadataFile getMetadataFile() {
		return null;
	}

	@Override
	public CorruptionPolicy getLogCorruptionPolicy() {
		return CorruptionPolicy.EXCEPTION;
	}
}
