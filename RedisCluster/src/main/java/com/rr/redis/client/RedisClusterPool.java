package com.rr.redis.client;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;

/**
 * <p>
 * JedisPool
 * </p>
 * 
 */
public class RedisClusterPool {

	private JedisPool pool;

	public RedisClusterPool(JedisPoolConfig jPoolCfg, String Addr, int port,
			int timeout) {
		this.pool = new JedisPool(jPoolCfg, Addr, port, timeout);
	}

	public void destroy() throws JedisException {
		this.pool.destroy();
	}

	public Jedis getResource() throws JedisException {
		Jedis j = this.pool.getResource();
		if (j != null) {
			return j;
		} else {
			throw new JedisException("Null getting Jedis Resource from Pool.");
		}
	}

	public void returnResource(Jedis jedis) throws JedisException {
		this.pool.returnResource(jedis);
	}

	public void returnBrokenResource(Jedis jedis) throws JedisException {
		this.pool.returnBrokenResource(jedis);
	}
}
