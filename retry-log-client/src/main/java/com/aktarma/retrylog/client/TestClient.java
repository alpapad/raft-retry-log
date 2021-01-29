package com.aktarma.retrylog.client;

import java.io.IOException;

public class TestClient {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		RetrylogClient client = new RetrylogClient();
		BeginCode code =	client.begin("aaa", 0, 10, 100);
		System.err.println(code);
		code =	client.begin("aaa", 0, 10, 100);
		System.err.println(code);
	}

}
