package com.rr.redis.client.zookeeper;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import com.netflix.curator.framework.CuratorFramework;
import com.rr.redis.client.RedisClusterPoolProvider;

/**
 * create(): 发起一个create操作. 可以组合其他方法 (比如mode 或background) 最后以forPath()方法结尾
 * 
 * 
 * delete(): 发起一个删除操作. 可以组合其他方法(version 或background) 最后以forPath()方法结尾
 * 
 * 
 * checkExists(): 发起一个检查ZNode 是否存在的操作. 可以组合其他方法(watch 或background)
 * 最后以forPath()方法结尾
 * 
 * 
 * getData(): 发起一个获取ZNode数据的操作. 可以组合其他方法(watch, background 或get stat)
 * 最后以forPath()方法结尾
 * 
 * 
 * setData(): 发起一个设置ZNode数据的操作. 可以组合其他方法(version 或background) 最后以forPath()方法结尾
 * 
 * 
 * getChildren(): 发起一个获取ZNode子节点的操作. 可以组合其他方法(watch, background 或get stat)
 * 最后以forPath()方法结尾
 * 
 * @author Administrator
 * 
 */
public class ZookeeperService {
	private static final Log logger = LogFactory.getLog(ZookeeperService.class);

	// 监听的地址的值变化情况
	public String WATCH_PATH = "";
	private String clusterName;

	private CuratorFramework client;
	private ZKClient zkClient;
	private RedisClusterPoolProvider provider;

	public ZookeeperService(String zkHosts, String clusterName, int zkTime) {
		WATCH_PATH = clusterName;
		this.clusterName = clusterName;
		zkClient = new ZKClient(zkHosts, zkTime);
		client = zkClient.getClient();
	}

	public void setProvider(RedisClusterPoolProvider provider) {
		this.provider = provider;
	}

	/**
	 * 监控WATCH_PATH，若该值在zk上有变化，则通知所有监听该值的warcher
	 */
	public void regWatcherOnLineRserver() {
		logger.info("[zk watcher] register Watcher " + WATCH_PATH);
		try {
			client.getData().usingWatcher(new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					logger.info("recieved zk change " + event.getPath() + " "
							+ event.getType().name());
					if (event.getType() == Event.EventType.NodeDataChanged) {
						try {
							byte[] b = client.getData().forPath(WATCH_PATH);

							String evt = new String(b, "utf-8");
							logger.info("update remote service config " + evt);

							updateClusterPool(Integer.valueOf(evt));
						} catch (Exception e) {
							logger.error(e.getMessage(), e);
						}
					}
					regWatcherOnLineRserver();
				}

				/**
				 * 更新连接池
				 * 
				 * 更新服务的时候，先把partition下面的节点更换，
				 * 然后再更新clusterName的值为改节点对应的partition
				 * 
				 * 
				 * 如果当前有请求过来，则会抛出异常，请注意!!!
				 * 
				 * @param partition
				 */
				private void updateClusterPool(Integer partition) {
					if (provider == null) {
						logger.error("Usage: provider is null!");
						return;
					}
					synchronized (provider) {
						try {
							provider.destroyClusterPool(partition);
							String partPath = clusterName + "/" + partition;
							List<String> nodes = getNodeList(partPath);
							// 建立连接池
							provider.createClusterPool(partition, nodes.get(0));
						} catch (Exception e) {
							logger.error(e.getMessage(), e);
						}
					}
				}
			}).forPath(WATCH_PATH);
		} catch (Exception e) {
			logger.error("zk watcher register error!" + e.getMessage(), e);
		}
	}

	/**
	 * 得到该节点下的所有子节点
	 * 
	 * @return
	 */
	public List<String> getNodeList(String path) {
		List<String> ret = new ArrayList<String>();
		try {
			// 得到该节点下的所有子节点
			ret = client.getChildren().forPath(path);
		} catch (Exception e) {
			logger.error(
					"get node list error! " + path + ".." + e.getMessage(), e);
		}

		return ret;
	}

	/**
	 * 得到某节点的状态，不存在则返回null
	 * 
	 * @param gid
	 * @return
	 * @throws Exception
	 */
	public Stat getStat(String path, String identify) throws Exception {
		// 得到某节点的状态，不存在则返回null
		return client.checkExists().forPath(path + "/" + identify);
	}

	/**
	 * 服务更新时请调用此方法
	 * 
	 * @throws UnsupportedEncodingException
	 * @throws Exception
	 */
	public void updateState(int evt) throws UnsupportedEncodingException,
			Exception {
		// 更新在zk上该地址的值
		client.setData().forPath(WATCH_PATH,
				String.valueOf(evt).getBytes("utf-8"));
	}

}
