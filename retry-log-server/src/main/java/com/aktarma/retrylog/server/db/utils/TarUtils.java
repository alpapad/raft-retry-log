package com.aktarma.retrylog.server.db.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for generating .tar.gz files.
 *
 * Ref:
 * https://memorynotfound.com/java-tar-example-compress-decompress-tar-tar-gz-files/
 */
public class TarUtils {
	private static final Logger LOG = LoggerFactory.getLogger(TarUtils.class);

	// generate .tar.gz file at the current working directory
	// tar.gz a folder
	public static void createTarFolder(Path source, String tarFileName) throws IOException {

		if (!Files.isDirectory(source)) {
			throw new IOException("Please provide a directory.");
		}

		try (OutputStream fOut = Files.newOutputStream(Paths.get(tarFileName));
				BufferedOutputStream buffOut = new BufferedOutputStream(fOut);
				TarArchiveOutputStream tOut = new TarArchiveOutputStream(buffOut)) {

			tOut.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR);
			// TAR originally does not support long file names, enable the support for it
			tOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
			tOut.setAddPaxHeadersForNonAsciiNames(true);
			
			Files.walkFileTree(source, new SimpleFileVisitor<Path>() {

				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
					// get filename
					Path targetFile = source.relativize(file);
					try {
						TarArchiveEntry tarEntry = new TarArchiveEntry(file.toFile(), targetFile.toString());
						tOut.putArchiveEntry(tarEntry);
						Files.copy(file, tOut);
						tOut.closeArchiveEntry();
						LOG.debug("Adding file {} to tar {}", file,  tarFileName);
					} catch (IOException e) {
						LOG.error("Error adding file {} to tar {}", file,  tarFileName, e);
						throw new RuntimeException(e);
					}
					return FileVisitResult.CONTINUE;
				}
				@Override
				public FileVisitResult visitFileFailed(Path file, IOException exc) {
					LOG.error("Error adding file {} to tar {}", file,  tarFileName, exc);
					throw new RuntimeException(exc);
				}
			});
			tOut.finish();
		}
	}
	/**
	 * Decompresses a tarball to one destination.
	 *
	 * @param in  the input file path
	 * @param out destination to decompress files to
	 */
	public static void extractTarToFolder(String in, File out) throws IOException {
		try (TarArchiveInputStream fin = new TarArchiveInputStream(new FileInputStream(in))) {//
			TarArchiveEntry entry;
			while ((entry = fin.getNextTarEntry()) != null) {
				if (entry.isDirectory()) {
					continue;
				}
				File curfile = new File(out, entry.getName());
				File parent = curfile.getParentFile();
				if (!parent.exists()) {
					parent.mkdirs();
				}
				IOUtils.copy(fin, new FileOutputStream(curfile));
			}
		}
	}
	/**
	 * Decompresses a tarball to one destination.
	 *
	 * @param in  the input file path
	 * @param out destination to decompress files to
	 */
	public static void decompress(String in, File out) throws IOException {
		try (TarArchiveInputStream fin = new TarArchiveInputStream(new FileInputStream(in))) {//
				//new BlockLZ4CompressorInputStream(new FileInputStream(in)))) {
			TarArchiveEntry entry;
			while ((entry = fin.getNextTarEntry()) != null) {
				if (entry.isDirectory()) {
					continue;
				}
				File curfile = new File(out, entry.getName());
				File parent = curfile.getParentFile();
				if (!parent.exists()) {
					parent.mkdirs();
				}
				IOUtils.copy(fin, new FileOutputStream(curfile));
			}
		}
	}
	
	/**
	 * Compresses a list of files to one destination.
	 *
	 * @param tarballName destination path of the .tar.gz file
	 * @param files       a list of files to add to the tarball
	 */
	public static void compress(String tarballName, File... files) throws IOException {
		try (TarArchiveOutputStream out = getTarArchiveOutputStream(tarballName)) {
			for (File file : files) {
				addToArchiveCompression(out, file, ".");
			}
		}
	}



	private static void addToArchiveCompression(TarArchiveOutputStream out, File file, String dir) throws IOException {
		String entry = dir + File.separator + file.getName();
		if (file.isDirectory()) {
			File[] children = file.listFiles();
			if (children != null && children.length > 0) {
				for (File child : children) {
					addToArchiveCompression(out, child, entry);
				}
			}
		} else {
			out.putArchiveEntry(new TarArchiveEntry(file, entry));
			try (FileInputStream in = new FileInputStream(file)) {
				IOUtils.copy(in, out);
			}
			out.closeArchiveEntry();
		}
	}

	private static TarArchiveOutputStream getTarArchiveOutputStream(String path) throws IOException {
		// Generate tar.gz file
		TarArchiveOutputStream taos = new TarArchiveOutputStream(
				new GzipCompressorOutputStream(new FileOutputStream(path)));
		// TAR has an 8G file limit by default, this gets around that
		taos.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR);
		// TAR originally does not support long file names, enable the support for it
		taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
		taos.setAddPaxHeadersForNonAsciiNames(true);
		return taos;
	}
}