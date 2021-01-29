package com.aktarma.retrylog.client;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.util.TimeDuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class ClientStressTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();


	@Test
	public void testOne() throws IOException {
	    MetricRegistries.global().enableConsoleReporter(TimeDuration.ONE_MINUTE);

		// TODO Auto-generated method stub
		RetrylogClient client = new RetrylogClient();
		
		String networkRef = UUID.randomUUID().toString();
		// byte[] byteArray = uuid();
		
		final int retry = 0;
		final long beginPhaseTimeout = 10;
		final long expiresAfter = 100;
		
		BeginCode added = client.begin(networkRef, retry, beginPhaseTimeout, expiresAfter);
		System.err.println(added);

		assertThat(added, is(BeginCode.OK));
		
		ResultCode commit =	client.commit(networkRef, retry, expiresAfter);
		assertThat(commit, is(ResultCode.OK));
		
		ResultCode confirmed = client.confirm(networkRef);
		assertThat(confirmed, is(ResultCode.OK));
		
	}
	@Test
	public void smTest() throws InterruptedException, IOException {
	    MetricRegistries.global().enableConsoleReporter(TimeDuration.ONE_MINUTE);

		RetrylogClient client = new RetrylogClient();
		ExecutorService ex = Executors.newFixedThreadPool(120);

		final int count = 2_000_000;
		
		long start = System.currentTimeMillis();
		int i = 1;
		for (int k = 0; k < count; k++) {
//			if (i > 0) {
//
//				if (i % 100 == 0) {
//
//					System.err.print('.');
//				}
//				if (i % 1000 == 0) {
//
//					System.err.print('\n');
//
//					long x = System.currentTimeMillis() - start;
//					if (x > 0) {
//						double tps = i / (x / 1000.01);
//						System.err.println("TPS:" + tps);
//					}
//					i = 0;
//					//start = System.currentTimeMillis();
//				}
//			}
			final int retry = 0;
			final long beginPhaseTimeout = 10;
			final long expiresAfter = 100;
			final int xx = k;
			CompletableFuture.supplyAsync(() -> {
				try {
					String networkRef = UUID.randomUUID().toString() + xx;
					// byte[] byteArray = uuid();
					
					BeginCode added = client.begin(networkRef, retry, beginPhaseTimeout, expiresAfter);
					
					if(!BeginCode.OK.equals(added)) {
						System.err.println("Begin got:" + added);
					}
					// assertThat(added, is(BeginCode.OK));
					
//					ResultCode commit =	client.commit(networkRef, retry, expiresAfter);
//					if(!ResultCode.OK.equals(commit)) {
//						System.err.println("Commit got:" + commit);
//					}

					
					// assertThat(commit, is(ResultCode.OK));
					
					ResultCode confirmed = client.confirm(networkRef);
					
					if(!ResultCode.OK.equals(confirmed)) {
						System.err.println("Confirm got:" + confirmed);
					}
					// assertThat(confirmed, is(ResultCode.OK));

				} catch (Exception e) {
					e.printStackTrace(System.err);
				}
				return null;
			}, ex);
//			i++;
		}

//				TransmissionBeginResponse.Code added2 = backend.begin(new RetryLogEntry(byteArray, 101, 121, 121, 1));
//				assertThat(added2, is(TransmissionBeginResponse.Code.INFLIGHT));
//				
//				OpResult code = backend.confirm(initial);
//				assertThat(code.getCode(), is(OperationResultCode.OK));

		System.err.println("Added " + count + " requests");
		ex.shutdown();
		ex.awaitTermination(100, TimeUnit.DAYS);
		
		long x = System.currentTimeMillis() - start;
		i = count;
		if (x > 0) {
			double tps = i / (x / 1000.01);
			System.err.println("TPS:" + tps + " tot sec:" + x/1000 + ", tot recs:" + count);
		}
		client.close();
		Thread.sleep(1000);
	}
}
