package com.aktarma.retrylog.server.db.rocksdb;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.aktarma.retrylog.common.wire.proto.TransmissionBeginRequest;
import com.aktarma.retrylog.common.wire.proto.TransmissionBeginResponse;
import com.aktarma.retrylog.common.wire.proto.TransmissionConfirmedRequest;
import com.aktarma.retrylog.server.db.IDbBackend;
import com.aktarma.retrylog.server.db.OpResult;
import com.aktarma.retrylog.server.db.OperationResultCode;
import com.aktarma.retrylog.server.db.TermIndexSupplier;
import com.aktarma.retrylog.server.db.TestRaftStorage;
import com.aktarma.retrylog.server.db.rocksdb.RocksDbBackend;

public class RocksDbBackendTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private final TermIndexSupplier sup = () -> TermIndex.valueOf(0, 0);

	@Test
	public void testDuplicate() throws IOException {
		final File path = tmp.newFolder();

		System.err.println(path);
		IDbBackend backend = new RocksDbBackend(sup);
		backend.init(new TestRaftStorage(path));
		try {
			byte[] byteArray = UUID.randomUUID().toString().getBytes();
			TransmissionBeginRequest beginRequest1 = TransmissionBeginRequest.newBuilder()
					.setNetworkref(ByteString.copyFrom(byteArray)).setRetry(0)//
					.setBeginPhaseExpires(0)//
					.setReftime(100).setExpires(120).build();

			TransmissionBeginResponse.Code added = backend.begin(1, 1, beginRequest1);
			assertThat(added, is(TransmissionBeginResponse.Code.OK));

			TransmissionConfirmedRequest confirmRq = TransmissionConfirmedRequest.newBuilder()
					.setNetworkref(ByteString.copyFrom(byteArray)).setReftime(121).build();

			backend.confirm(1, 2, confirmRq);

			TransmissionBeginRequest rq3 = TransmissionBeginRequest.newBuilder()
					.setNetworkref(ByteString.copyFrom(byteArray)).setRetry(0)//
					.setBeginPhaseExpires(0)//
					.setReftime(121).setExpires(140).build();

			TransmissionBeginResponse.Code added3 = backend.begin(1, 3, rq3);

			assertThat(added3, is(TransmissionBeginResponse.Code.DUPLICATE));
			System.err.println(added + "   " + added3);
		} finally {
			backend.close();
		}
	}

	@Test
	public void testExpired() throws IOException {
		final File path = tmp.newFolder();

		System.err.println(path);
		IDbBackend backend = new RocksDbBackend(sup);
		backend.init(new TestRaftStorage(path));
		
		
		try {
			byte[] byteArray = UUID.randomUUID().toString().getBytes();

			TransmissionBeginRequest beginRequest1 = TransmissionBeginRequest.newBuilder()
					.setNetworkref(ByteString.copyFrom(byteArray))//
					.setRetry(0)//
					.setBeginPhaseExpires(120)//
					.setReftime(100)//
					.setExpires(120)//
					.build();

			TransmissionBeginResponse.Code added = backend.begin(1,1,beginRequest1);
			assertThat(added, is(TransmissionBeginResponse.Code.OK));

			TransmissionBeginRequest beginRequest2 = TransmissionBeginRequest.newBuilder()
					.setNetworkref(ByteString.copyFrom(byteArray))//
					.setRetry(1)//
					.setBeginPhaseExpires(140)//
					.setReftime(110)//
					.setExpires(141)//
					.build();

			TransmissionBeginResponse.Code added2 = backend.begin(1,2,beginRequest2);
			assertThat(added2, is(TransmissionBeginResponse.Code.INFLIGHT));

			TransmissionBeginRequest beginRequest3 = TransmissionBeginRequest.newBuilder()
					.setNetworkref(ByteString.copyFrom(byteArray))//
					.setRetry(1)//
					.setBeginPhaseExpires(140)//
					.setReftime(121)//
					.setExpires(140)//
					.build();

			TransmissionBeginResponse.Code added3 = backend.begin(1,3,beginRequest3);

			assertThat(added3, is(TransmissionBeginResponse.Code.OK));
			System.err.println(added + "   " + added2 + " " + added3);
		} finally {
			backend.close();
		}
	}

	@Test
	public void testEncoding() throws DecoderException {
		UUID u = UUID.randomUUID();

		byte[] byteArray = u.toString().replace("-", "").getBytes();

		byte[] byteArray2 = org.apache.commons.codec.binary.Base64.decodeBase64(byteArray);
		byte[] b3 = Hex.decodeHex(u.toString().replace("-", ""));

		System.err.println("L1:" + byteArray.length + " ---  L2:" + byteArray2.length + " --- L4:" + b3.length);
	}

	static byte[] uuid() {
		try {
			return Hex.decodeHex(UUID.randomUUID().toString().replace("-", ""));
		} catch (DecoderException e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	public void testSnapshot() throws IOException {
		final File path = new File("/home/tests/" + UUID.randomUUID().toString());
		path.mkdirs();
		
		AtomicLong index = new AtomicLong(0);
		
		AtomicLong l = new AtomicLong(0);

		System.err.println(path);
		IDbBackend backend = new RocksDbBackend(() -> TermIndex.valueOf(0, l.get()));
		backend.init(new TestRaftStorage(path));
		try {

			AtomicLong start = new AtomicLong(System.currentTimeMillis());

			IntStream.range(0, 100_000_001).forEach(i -> {
				l.incrementAndGet();
				if (i > 0) {

					if (i % 10000 == 0) {

						System.err.print('.');
//						backend.sync();
					}
					if (i % 1000000 == 0) {

						backend.info();
						System.err.print('\n');

						long x = System.currentTimeMillis() - start.get();
						//backend.sync();

						long tps = l.get() / (x / 1000);
						System.err.println("TPS:" + tps);
						l.set(0);
						start.set(System.currentTimeMillis());
					}
				}
				byte[] byteArray = uuid();

				// RetryLogEntry initial = new RetryLogEntry(byteArray, 100, 120, 120, 0);

				TransmissionBeginRequest initial = TransmissionBeginRequest.newBuilder()
						.setNetworkref(ByteString.copyFrom(byteArray))//
						.setRetry(0)//
						.setBeginPhaseExpires(120)//
						.setReftime(100)//
						.setExpires(120)//
						.build();

				TransmissionBeginResponse.Code added = backend.begin(1, index.incrementAndGet(), initial);
				assertThat(added, is(TransmissionBeginResponse.Code.OK));

				TransmissionBeginRequest second = TransmissionBeginRequest.newBuilder()
						.setNetworkref(ByteString.copyFrom(byteArray))//
						.setRetry(1)//
						.setBeginPhaseExpires(121)//
						.setReftime(101)//
						.setExpires(121)//
						.build();

				TransmissionBeginResponse.Code added2 = backend.begin(1, index.incrementAndGet(), second);
				assertThat(added2, is(TransmissionBeginResponse.Code.INFLIGHT));

				TransmissionConfirmedRequest confirmRq = TransmissionConfirmedRequest.newBuilder()
						.setNetworkref(ByteString.copyFrom(byteArray))//
						.setReftime(121)//
						.build();

				OpResult code = backend.confirm(1, index.incrementAndGet(), confirmRq);
				assertThat(code.getCode(), is(OperationResultCode.OK));
			});
			System.err.println("");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			final File backup = tmp.newFolder();
			File backupFile = new File(backup, "backup.mdb");

			System.err.println(backupFile);
			backend.takeSnapshot();
			backend.close();
		}
	}
}
