package com.rr.redis.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.rr.redis.client.RedisClusterPoolClient;

public class TestRedisService {
	public static String zkHost = "10.4.28.172:2181,10.4.28.179:2181";
	public static String clusterName = "pubilc.remote";

	public static void main(String[] args) throws Exception {
		RedisClusterPoolClient client = new RedisClusterPoolClient(clusterName,
				zkHost);
		client.init();

		String[] keys = new String[20];
		List<String> ks = new ArrayList<String>();
		byte[][] keys2 = new byte[20][];
		List<byte[]> ks2 = new ArrayList<byte[]>();
		Map<String, String> mat = new HashMap<String, String>();
		Map<byte[], byte[]> mat2 = new HashMap<byte[], byte[]>();
		while (true) {
			for (int i = 0; i < 20; i++) {
				try {

					String str = "test_" + i;
					// System.out.println("hash:" + str + "=" + getHash(str));
					// client.set(str, "" + i);
					// mat.put(str, "" + i);
					// client.get(str);
					// client.set(str.getBytes(), ("" + i).getBytes());
					// mat2.put(str.getBytes(), ("" + i).getBytes());
					// System.out.println(client.type(str));
					keys[i] = str;
					ks.add(str);
					keys2[i] = str.getBytes();
					ks2.add(str.getBytes());

				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			try {
				System.out.println(client.mget(keys));
			} catch (Exception e) {
				e.printStackTrace();
			}
			Thread.sleep(300);
		}
		// client.del(keys);
		// client.del(ks);
		// client.del(keys2);
		// System.out.println(client.mget(keys));
		// System.out.println(new String(client.mget(keys2).get(16)));
		// client.delBinary(ks2);

		// client.flushAll();

		// client.mset(mat);
		// client.msetnx(mat);

		// client.msetBinary(mat2);

		// System.exit(0);
	}

	private static int getHash(String key) {
		return Math.abs(key.hashCode() % 4);
	}
}
