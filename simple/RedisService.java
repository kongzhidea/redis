package com.kk.service;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Redis操作接口 连接池
 *
 */
public class RedisService {
    private final Log logger = LogFactory.getLog(RedisService.class);

    private JedisPool pool = null;

    private String host;
    private int port;

    private int jPoolCfgMaxActive;
    private int jPoolCfgMaxIdle;
    private int jPoolCfgMaxWait;
    private String password;// 密码

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getjPoolCfgMaxActive() {
        return jPoolCfgMaxActive;
    }

    public void setjPoolCfgMaxActive(int jPoolCfgMaxActive) {
        this.jPoolCfgMaxActive = jPoolCfgMaxActive;
    }

    public int getjPoolCfgMaxIdle() {
        return jPoolCfgMaxIdle;
    }

    public void setjPoolCfgMaxIdle(int jPoolCfgMaxIdle) {
        this.jPoolCfgMaxIdle = jPoolCfgMaxIdle;
    }

    public int getjPoolCfgMaxWait() {
        return jPoolCfgMaxWait;
    }

    public void setjPoolCfgMaxWait(int jPoolCfgMaxWait) {
        this.jPoolCfgMaxWait = jPoolCfgMaxWait;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * 构建redis连接池 初始化
     */
    public void init() {
        JedisPoolConfig config = new JedisPoolConfig();
        // 控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
        // 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
        config.setMaxTotal(jPoolCfgMaxActive);
        // 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
        config.setMaxIdle(jPoolCfgMaxIdle);
        // 表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
        config.setMaxWaitMillis(jPoolCfgMaxWait);
        // 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
        config.setTestOnBorrow(true);
        if(StringUtils.isBlank(password)) {
            pool = new JedisPool(config, host, port, 2000);
        }else {
            pool = new JedisPool(config, host, port, 2000, password);
        }
    }

    private JedisPool getPool() {
        return pool;
    }

    /**
     * 返还到连接池
     *
     * @param pool
     * @param redis
     */
    public void returnResource(JedisPool pool, Jedis redis) {
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
    public String get(String key) {
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


    /**
     * 获取枚举类型key的值,适用于get/set
     *
     * @param key
     * @return
     */
    public int getInt(String key) {
        try {
            return Integer.parseInt(get(key));
        } catch (Exception e) {
            return 0;
        }
    }

    // 如果key不存在则返回1，存在则返回0
    public long setnx(String key, String value) {
        JedisPool pool = null;
        Jedis jedis = null;
        long ret = 0;
        try {
            pool = getPool();
            jedis = pool.getResource();

            ret = jedis.setnx(key, value);
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

    public void set(String key, String value) {
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

    public boolean exists(String key) {
        boolean value = false;

        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();

            value = jedis.exists(key);
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

    public void delete(String... keys) {
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

    public void setex(String key, int seconds, String value) {
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

    public long ttl(String key) {
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
    public void lpush(String key, String value) {
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

    public void rpush(String key, String value) {
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

    public void lpop(String key) {
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

    public void rpop(String key) {
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

    public long llen(String key) {
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
    public List<String> lrange(String key, long start, long end) {
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

    /**
     * Returns all the keys matching the glob-style pattern as space separated
     * strings
     *
     * @param pattern
     * @return
     */
    public Set<String> keys(String pattern) {
        Set<String> set = new HashSet<String>();
        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();

            set = jedis.keys(pattern);
        } catch (Exception e) {
            // 释放redis对象
            pool.returnBrokenResource(jedis);
            logger.error(e.getMessage(), e);
        } finally {
            // 返还到连接池
            returnResource(pool, jedis);
        }
        return set;
    }

    /**
     * Return the number of keys in the currently selected database.
     *
     *
     * @return
     */
    public long dbSize() {
        Long ret = 0l;
        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();

            ret = jedis.dbSize();
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

}