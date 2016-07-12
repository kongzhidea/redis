package com.rr.redis.simple;

import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;

/**
 * 没有使用连接池 不推荐使用
 * 
 * @author kk
 * 
 */
public class RedisUtil {
	private static RedisUtil _instance = null;
	private static Jedis jedis;
	private static final String host = "10.4.28.172";
	private static final int port = 6379;

	private RedisUtil() {
		jedis = new Jedis(host, port);
	}

	public static RedisUtil getInstance() {
		if (_instance == null) {
			synchronized (RedisUtil.class) {
				if (_instance == null) {
					_instance = new RedisUtil();
				}
			}
		}
		return _instance;
	}

	public String get(String key) {
		return jedis.get(key);
	}

	/********************** 字符串 ****************/
	public void set(String key, int value) {
		jedis.set(key, String.valueOf(value));
	}

	public void set(String key, String value) {
		jedis.set(key, value);
	}

	public void delete(String... keys) {
		jedis.del(keys);
	}

	public void setex(String key, int seconds, String value) {
		jedis.setex(key, seconds, value);
	}

	public long ttl(String key) {
		return jedis.ttl(key);
	}

	/********************* 列表 ****************/
	public void lpush(String key, String value) {
		jedis.lpush(key, value);
	}

	public void lpush(String key, String... value) {
		jedis.lpush(key, value);
	}

	public void rpush(String key, String value) {
		jedis.rpush(key, value);
	}

	public void rpush(String key, String... value) {
		jedis.rpush(key, value);
	}

	public void lpop(String key) {
		jedis.lpop(key);
	}

	public void rpop(String key) {
		jedis.rpop(key);
	}

	public long llen(String key) {
		return jedis.llen(key);
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
	public List<String> lrange(String key, long start, long end) {
		return jedis.lrange(key, start, end);
	}

	/************************ 有序列表 *****************/
	/**
	 * 添加元素
	 * 
	 * @param key
	 * @param score
	 * @param member
	 */
	public void zadd(String key, double score, String member) {
		jedis.zadd(key, score, member);
	}

	/**
	 * 得到member排名
	 * 
	 * @param key
	 * @param member
	 * @return 从0开始,-1表示不存在
	 */
	public long zrank(String key, String member) {
		Long ret = jedis.zrank(key, member);
		if (ret == null) {
			return -1;
		}
		return ret;
	}

	/**
	 * 得到member倒序排名
	 * 
	 * @param key
	 * @param member
	 * @return 从0开始,-1表示不存在
	 */
	public long zrevrank(String key, String member) {
		Long ret = jedis.zrevrank(key, member);
		if (ret == null) {
			return -1;
		}
		return ret;
	}

	/**
	 * 集合size
	 * 
	 * @param key
	 * @return
	 */
	public long zcard(String key) {
		return jedis.zcard(key);
	}

	/**
	 * 删除member
	 * 
	 * @param key
	 * @param members
	 */
	public void zrem(String key, String... members) {
		jedis.zrem(key, members);
	}

	/**
	 * 得到分数
	 * 
	 * @param key
	 * @param member
	 * @return
	 */
	public double zscore(String key, String member) {
		Double ret = jedis.zscore(key, member);
		if (ret == null) {
			return 0.0;
		}
		return ret;
	}

	/**
	 * 返回的成员在排序设置的范围
	 * 
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public Set<String> zrange(String key, long start, long end) {
		return jedis.zrange(key, start, end);
	}

	/**
	 * 在排序的设置返回的成员范围，通过索引，下令从分数高到低
	 * 
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public Set<String> zrevrange(String key, long start, long end) {
		return jedis.zrevrange(key, start, end);
	}

	/**
	 * 返回指定分数段内的集合元素个数
	 * 
	 * @param key
	 * @param min
	 * @param max
	 * @return
	 */
	public long zcount(String key, double min, double max) {
		return jedis.zcount(key, min, max);
	}

	/**
	 * 向member增加/减少score
	 * 
	 * @param key
	 * @param score
	 * @param member
	 * @return
	 */
	public double zincrby(String key, double score, String member) {
		return jedis.zincrby(key, score, member);
	}

}
