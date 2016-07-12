package com.rr.redis.client.zookeeper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

public class ZKClient {
	public static Log logger = LogFactory.getLog(ZKClient.class);
	private CuratorFramework client;

	// private static String zkHosts = "10.4.28.172:2181,10.4.28.179:2181";

	// 命名空间 zkClient下所有的data都在该地址下 zk中使用 '/' 可以指定目录结构
	private String namespace = "redis";
	private int zkTime = 5 * 60 * 1000;

//	public ZKClient(String zkHosts) {
//		init(zkHosts, namespace, zkTime);
//	}

	public ZKClient(String zkHosts, int zkTime) {
		init(zkHosts, namespace, zkTime);
	}

//	public ZKClient(String zkHosts, String namespace) {
//		init(zkHosts, namespace, zkTime);
//	}
//
//	public ZKClient(String zkHosts, String namespace, int time) {
//		init(zkHosts, namespace, time);
//	}

	public void init(String zkHosts, String namespace, int time) {
		try {
			logger.info("zkHosts: " + zkHosts);
			client = CuratorFrameworkFactory.builder().connectString(zkHosts)
					.namespace(namespace)
					.retryPolicy(new RetryNTimes(Integer.MAX_VALUE, time))
					.connectionTimeoutMs(5000).build();
		} catch (IOException e) {
			logger.error("get client error", e);
		}
		client.start();
	}

	public CuratorFramework getClient() {
		return client;
	}

	public void destroy() throws Exception {
		client.close();
	}

}
