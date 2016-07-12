package com.rr.redis.client;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;

import com.rr.redis.client.hash.IHashFunc;
import com.rr.redis.client.hash.SimpleHashing;
import com.rr.redis.client.model.Node;
import com.rr.redis.client.zookeeper.ZookeeperService;

public class RedisClusterPoolProvider {
	private static final Log logger = LogFactory
			.getLog(RedisClusterPoolProvider.class);
	/**
	 * key:redis中key的hash值,value:对应的服务的连接池，每个连接池中有个JedisPool
	 * 
	 * 由于是根据key的hash来分布，所以redis服务的数量不能改变
	 * 
	 * zk上样例:pubilc.remote/1/10.4.28.172:6379->clusterName:pubilc.remote,key:1(
	 * 从0开始), value:10.4.28.172:6379的连接池
	 */
	private Map<Integer, RedisClusterPool> pools;
	private static int jPoolCfgMaxActive = 1000;
	private static int jPoolCfgMaxIdle = 100;
	private static int jPoolCfgMaxWait = 1000;

	/**
	 * Number of hash slot(node).
	 */
	private int hashSlotNum;
	private IHashFunc hashFunc;
	private int redisTimeOut;

	private ZookeeperService zookeeper;

	/**
	 * 构造函数，包含初始化过程
	 * 
	 * @param clusterName
	 * @param zkHost
	 * @param zkTimeout
	 * @param redisTimeout
	 */
	public RedisClusterPoolProvider(String clusterName, String zkHost,
			int zkTimeout, int redisTimeout) {

		zookeeper = new ZookeeperService(zkHost, clusterName, zkTimeout);
		zookeeper.setProvider(this);// 监听zk变化时候 执行update方法
		zookeeper.regWatcherOnLineRserver();// 开始监听zk

		redisTimeOut = redisTimeout;
		pools = new ConcurrentHashMap<Integer, RedisClusterPool>();

		init(clusterName);
	}

	/**
	 * 初始化过程: 从zk上获取节点，并建立连接池,初始化hash算法
	 * 
	 * @param clusterName
	 */
	private void init(String clusterName) {
		List<String> parts = zookeeper.getNodeList(clusterName);
		this.hashSlotNum = parts.size();
		this.hashFunc = new SimpleHashing(hashSlotNum);
		for (String part : parts) {
			int pat = Integer.valueOf(part);
			try {
				String partPath = clusterName + "/" + part;
				List<String> nodes = zookeeper.getNodeList(partPath);
				// 建立连接池
				createClusterPool(pat, nodes.get(0));
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				destroyClusterPool(pat);
			}
		}
	}

	/**
	 * 建立连接池
	 * 
	 * @param partition
	 * @param identity
	 */
	public void createClusterPool(int partition, String identity) {
		logger.info("create pool:" + partition + ".." + identity);
		Node node = Node.getNodeFromIdentity(identity);
		if (!this.pools.containsKey(partition)) {
			RedisClusterPool pool = new RedisClusterPool(getJedisPoolConfig(),
					node.getHost(), node.getPort(), redisTimeOut);
			this.pools.put(partition, pool);
		}
	}

	/**
	 * 销毁连接池
	 * 
	 * @param partition
	 */
	public void destroyClusterPool(int partition) {
		if (this.pools != null && this.pools.containsKey(partition)) {
			try {
				this.pools.get(partition).destroy();
				this.pools.remove(partition);
				logger.info("destroy pool:" + partition);
			} catch (JedisException e) {
				logger.error("fail destroy ClusterPool", e);
			}
		}
	}

	/**
	 * 得到hash算法
	 * 
	 * @return
	 */
	public IHashFunc getHashFunc() {
		return hashFunc;
	}

	/**
	 * 设置连接池配置项
	 * 
	 * @param maxActive
	 * @param maxIdle
	 * @param maxWait
	 */
	public static void setJedisPoolConfig(int maxActive, int maxIdle,
			int maxWait) {
		jPoolCfgMaxActive = maxActive;
		jPoolCfgMaxIdle = maxIdle;
		jPoolCfgMaxWait = maxWait;
	}

	/**
	 * 得到 连接池配置项
	 * 
	 * @return
	 */
	public static JedisPoolConfig getJedisPoolConfig() {
		JedisPoolConfig config = new JedisPoolConfig();
		// 控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
		// 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
		config.setMaxActive(jPoolCfgMaxActive);
		// 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
		config.setMaxIdle(jPoolCfgMaxIdle);
		// 表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
		config.setMaxWait(jPoolCfgMaxWait);
		// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
		config.setTestOnBorrow(false);
		return config;
	}

	/**
	 * 得到redis所有的partition
	 */
	public Collection<Integer> getAllPartitions() {
		return pools.keySet();
	}

	/**
	 * 得到所有的连接池
	 * 
	 * @return
	 */
	public Map<Integer, RedisClusterPool> getPoolsMap() {
		return pools;
	}

	/**
	 * 得到key对应的连接池
	 * 
	 * @param partition
	 *            key的hash值
	 * @return
	 */
	public RedisClusterPool getPool(int partition) {
		return pools.get(partition);
	}

	/**
	 * 得到redis连接池的数量
	 * 
	 * @return
	 */
	public int getPoolsLen() {
		return pools.size();
	}

}
