package com.aktarma.retrylog.client;

import static org.apache.ratis.util.FileUtils.createDirectories;
import static org.apache.ratis.util.FileUtils.deleteFully;
import static org.apache.ratis.util.FileUtils.move;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.junit.Test;
import org.slf4j.Logger;

public class MoveFileTest {
	static Logger LOG = org.apache.ratis.util.FileUtils.LOG;

	@Test
	public void test() throws IOException {
		org.apache.ratis.util.FileUtils.moveDirectory(new File("/home/tests/tt1/b/").toPath(), new File("/home/tests/tt1/a").toPath());
		org.apache.ratis.util.FileUtils.deleteFully(new File("/home/tests/tt1/b/").toPath());
	}

	/**
	 * Moves the directory. If any file is locked, the exception is caught and
	 * logged and continues to other files.
	 * 
	 * @param source
	 * @param dest
	 * @throws IOException
	 */
	static void moveDirectory(Path source, Path dest) throws IOException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("moveDirectory source: {} dest: {}", source, dest);
		}
		createDirectories(dest);
		Files.walkFileTree(source, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
				Path targetPath = dest.resolve(source.relativize(dir));
				if (!Files.exists(targetPath)) {
					createDirectories(targetPath);
				}
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				dir.toFile().delete();
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
				try {
					move(file, dest.resolve(source.relativize(file)));
				} catch (IOException e) {
					LOG.info("Files.moveDirectory: could not delete {}", file.getFileName());
				}
				return FileVisitResult.CONTINUE;
			}
		});

		/* Delete the source since all the files has been moved. */
		deleteFully(source);
	}
}
