package com.rr.redis.simple;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Redis操作接口 连接池，但是没有实现连接集群
 * 
 */
public class RedisAPI {
	private static JedisPool pool = null;

	private static final String host = "10.4.28.172";
	private static final int port = 6379;

	public static final Log logger = LogFactory.getLog(RedisAPI.class);

	private static int jPoolCfgMaxActive = 1000;
	private static int jPoolCfgMaxIdle = 100;
	private static int jPoolCfgMaxWait = 1000;

	/**
	 * 构建redis连接池
	 * 
	 * @param ip
	 * @param port
	 * @return JedisPool
	 */

	public static JedisPool getPool() {
		if (pool == null) {
			synchronized (RedisAPI.class) {
				if (pool == null) {
					JedisPoolConfig config = new JedisPoolConfig();
					// 控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
					// 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
					config.setMaxActive(jPoolCfgMaxActive);
					// 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
					config.setMaxIdle(jPoolCfgMaxIdle);
					// 表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
					config.setMaxWait(jPoolCfgMaxWait);
					// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
					config.setTestOnBorrow(true);
					pool = new JedisPool(config, host, port, 2000);
				}
			}

		}
		return pool;
	}

	/**
	 * 返还到连接池
	 * 
	 * @param pool
	 * @param redis
	 */
	public static void returnResource(JedisPool pool, Jedis redis) {
		if (redis != null) {
			pool.returnResource(redis);
		}
	}

	/**
	 * 获取数据
	 * 
	 * @param key
	 * @return
	 */
	public static String get(String key) {
		String value = null;

		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			value = jedis.get(key);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}

		return value;
	}

	public static void set(String key, String value) {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			jedis.set(key, value);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
	}

	public static void set(String key, int value) {
		set(key, String.valueOf(value));
	}

	public static void delete(String... keys) {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			jedis.del(keys);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
	}

	public static void setex(String key, int seconds, String value) {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			jedis.setex(key, seconds, value);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
	}

	public static long ttl(String key) {
		long t = 0;
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			t = jedis.ttl(key);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
		return t;
	}

	/********************* 列表 ****************/
	public static void lpush(String key, String value) {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			jedis.lpush(key, value);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
	}

	public static void lpush(String key, String... value) {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			jedis.lpush(key, value);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
	}

	public static void rpush(String key, String value) {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			jedis.rpush(key, value);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
	}

	public static void rpush(String key, String... value) {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			jedis.rpush(key, value);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
	}

	public static void lpop(String key) {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			jedis.lpop(key);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
	}

	public static void rpop(String key) {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			jedis.rpop(key);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
	}

	public static long llen(String key) {
		Long len = null;
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			len = jedis.llen(key);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
		return len == null ? 0 : len;
	}

	/**
	 * 
	 * @param key
	 * @param start
	 *            从0开始
	 * @param end
	 *            -1表示到末尾, 取值范围[start,end]
	 * @return
	 */
	public static List<String> lrange(String key, long start, long end) {
		List<String> list = new ArrayList<String>();
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			list = jedis.lrange(key, start, end);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
		return list;
	}

	/************************ 有序列表 *****************/
	/**
	 * 添加元素
	 * 
	 * @param key
	 * @param score
	 * @param member
	 */
	public static void zadd(String key, double score, String member) {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			jedis.zadd(key, score, member);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
	}

	/**
	 * 得到member排名
	 * 
	 * @param key
	 * @param member
	 * @return 从0开始,-1表示不存在
	 */
	public static long zrank(String key, String member) {
		Long ret = null;
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			ret = jedis.zrank(key, member);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
		return ret == null ? -1 : ret;
	}

	/**
	 * 得到member倒序排名
	 * 
	 * @param key
	 * @param member
	 * @return 从0开始,-1表示不存在
	 */
	public static long zrevrank(String key, String member) {
		Long ret = null;
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			ret = jedis.zrevrank(key, member);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
		return ret == null ? -1 : ret;
	}

	/**
	 * 集合size
	 * 
	 * @param key
	 * @return
	 */
	public static long zcard(String key) {
		Long len = null;
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			len = jedis.zcard(key);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
		return len == null ? 0 : len;
	}

	/**
	 * 删除member
	 * 
	 * @param key
	 * @param members
	 */
	public static void zrem(String key, String... members) {
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			jedis.zrem(key, members);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
	}

	/**
	 * 得到分数
	 * 
	 * @param key
	 * @param member
	 * @return
	 */
	public static double zscore(String key, String member) {
		Double ret = null;
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			ret = jedis.zscore(key, member);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
		return ret == null ? 0.0 : ret;
	}

	/**
	 * 返回的成员在排序设置的范围
	 * 
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public static Set<String> zrange(String key, long start, long end) {
		Set<String> ret = new HashSet<String>();
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			ret = jedis.zrange(key, start, end);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
		return ret;
	}

	/**
	 * 在排序的设置返回的成员范围，通过索引，下令从分数高到低
	 * 
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public static Set<String> zrevrange(String key, long start, long end) {
		Set<String> ret = new HashSet<String>();
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			ret = jedis.zrevrange(key, start, end);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
		return ret;
	}

	/**
	 * 返回指定分数段内的集合元素个数
	 * 
	 * @param key
	 * @param min
	 * @param max
	 * @return
	 */
	public static long zcount(String key, double min, double max) {
		Long len = null;
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			len = jedis.zcount(key, min, max);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
		return len == null ? 0 : len;
	}

	/**
	 * 向member增加/减少score
	 * 
	 * @param key
	 * @param score
	 * @param member
	 * @return
	 */
	public static double zincrby(String key, double score, String member) {
		Double ret = null;
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = getPool();
			jedis = pool.getResource();

			ret = jedis.zincrby(key, score, member);
		} catch (Exception e) {
			// 释放redis对象
			pool.returnBrokenResource(jedis);
			logger.error(e.getMessage(), e);
		} finally {
			// 返还到连接池
			returnResource(pool, jedis);
		}
		return ret == null ? 0.0 : ret;
	}

}