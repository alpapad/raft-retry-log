package com.aktarma.retrylog.server.db.lmdb;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.AtomicFileOutputStream;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.MD5FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LmdbStorageHelper {
	private static final Logger LOG = LoggerFactory.getLogger(LmdbStorageHelper.class);

	static final String SNAPSHOT_FILE_PREFIX = "lmdb_snapshot";
	static final String CORRUPT_SNAPSHOT_FILE_SUFFIX = ".corrupt";
	/** snapshot.term_index */
	public static final Pattern SNAPSHOT_REGEX = Pattern.compile(SNAPSHOT_FILE_PREFIX + "\\.(\\d+)_(\\d+)");

	private File smDir = null;

	public LmdbStorageHelper(File smDir) {
		super();
		this.smDir = smDir;
	}

	public void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy){
		CompletableFuture.supplyAsync(() -> {
			try {
				if (snapshotRetentionPolicy != null && snapshotRetentionPolicy.getNumSnapshotsRetained() > 0) {

					List<SingleFileSnapshotInfo> allSnapshotFiles = new ArrayList<>();
					try (DirectoryStream<Path> stream = Files.newDirectoryStream(smDir.toPath())) {
						for (Path path : stream) {
							Matcher matcher = SNAPSHOT_REGEX.matcher(path.getFileName().toString());
							if (matcher.matches()) {
								final long endIndex = Long.parseLong(matcher.group(2));
								final long term = Long.parseLong(matcher.group(1));
								final FileInfo fileInfo = new FileInfo(path, null); // We don't need FileDigest here.
								allSnapshotFiles.add(new SingleFileSnapshotInfo(fileInfo, term, endIndex));
							}
						}
					}

					if (allSnapshotFiles.size() > snapshotRetentionPolicy.getNumSnapshotsRetained()) {
						allSnapshotFiles.sort(new SnapshotFileComparator());
						List<File> snapshotFilesToBeCleaned = allSnapshotFiles
								.subList(snapshotRetentionPolicy.getNumSnapshotsRetained(), allSnapshotFiles.size())
								.stream()
								.map(singleFileSnapshotInfo -> singleFileSnapshotInfo.getFile().getPath().toFile())
								.collect(Collectors.toList());
						for (File snapshotFile : snapshotFilesToBeCleaned) {
							LOG.info("Deleting old snapshot at {}", snapshotFile.getAbsolutePath());
							FileUtils.deleteFileQuietly(snapshotFile);
							FileUtils.deleteFileQuietly(new File(snapshotFile.getAbsolutePath() + ".md5"));
						}
					}
				}
				return null;
			} catch (IOException ex) {
				LOG.error("Error cleaning old snapshots", ex);
				return null;
			}
		});
	}

	public static TermIndex getTermIndexFromSnapshotFile(File file) {
		final String name = file.getName();
		final Matcher m = SNAPSHOT_REGEX.matcher(name);
		if (!m.matches()) {
			throw new IllegalArgumentException(
					"File \"" + file + "\" does not match snapshot file name pattern \"" + SNAPSHOT_REGEX + "\"");
		}
		final long term = Long.parseLong(m.group(1));
		final long index = Long.parseLong(m.group(2));
		return TermIndex.valueOf(term, index);
	}

	protected static String getTmpSnapshotFileName(long term, long endIndex) {
		return getSnapshotFileName(term, endIndex) + AtomicFileOutputStream.TMP_EXTENSION;
	}

	protected static String getCorruptSnapshotFileName(long term, long endIndex) {
		return getSnapshotFileName(term, endIndex) + CORRUPT_SNAPSHOT_FILE_SUFFIX;
	}

	public File getSnapshotFile(long term, long endIndex) {
		return new File(smDir, getSnapshotFileName(term, endIndex));
	}

	protected File getTmpSnapshotFile(long term, long endIndex) {
		return new File(smDir, getTmpSnapshotFileName(term, endIndex));
	}

	protected File getCorruptSnapshotFile(long term, long endIndex) {
		return new File(smDir, getCorruptSnapshotFileName(term, endIndex));
	}

	public SingleFileSnapshotInfo findLatestSnapshot() {
		try {
			SingleFileSnapshotInfo latest = null;
			try (DirectoryStream<Path> stream = Files.newDirectoryStream(smDir.toPath())) {
				for (Path path : stream) {
					Matcher matcher = SNAPSHOT_REGEX.matcher(path.getFileName().toString());
					if (matcher.matches()) {
						final long endIndex = Long.parseLong(matcher.group(2));
						if (latest == null || endIndex > latest.getIndex()) {
							final long term = Long.parseLong(matcher.group(1));
							MD5Hash fileDigest = MD5FileUtil.readStoredMd5ForFile(path.toFile());
							final FileInfo fileInfo = new FileInfo(path, fileDigest);
							latest = new SingleFileSnapshotInfo(fileInfo, term, endIndex);
						}
					}
				}
			}
			return latest;
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	public static String getSnapshotFileName(long term, long endIndex) {
		return SNAPSHOT_FILE_PREFIX + "." + term + "_" + endIndex;
	}

	/**
	 * Compare snapshot files based on transaction indexes.
	 */
	class SnapshotFileComparator implements Comparator<SingleFileSnapshotInfo> {
		@Override
		public int compare(SingleFileSnapshotInfo file1, SingleFileSnapshotInfo file2) {
			return (int) (file2.getIndex() - file1.getIndex());
		}
	}
}
