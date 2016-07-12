package com.rr.redis.test;

import com.rr.redis.client.zookeeper.ZookeeperService;

public class ZKTest {
	public static void main(String[] args) {
		String clusterName = "pubilc.remote";
		String zkHost = "10.4.28.172:2181,10.4.28.179:2181";

		// 更新服务的时候，先把partition下面的节点更换，然后再更新clusterName的值为改节点对应的partition
		//更新的时候  如果当前有请求过来，则会抛出异常，请注意!!!
		int zkTimeout = 2000;
		ZookeeperService zookeeper = new ZookeeperService(zkHost, clusterName,
				zkTimeout);
		try {
			zookeeper.updateState(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}
}
