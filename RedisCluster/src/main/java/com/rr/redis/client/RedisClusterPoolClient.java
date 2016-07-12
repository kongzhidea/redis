package com.rr.redis.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;

import com.rr.redis.client.exception.ClusterOpException;
import com.rr.redis.client.hash.IHashFunc;

/**
 * <p>
 * Pool Client for redis cluster.
 * </p>
 * <p>
 * This class is thread safe since we use JedisPool.
 * 
 * USAGE: private RedisClusterPoolClient client; client = new
 * RedisClusterPoolClient("testCluster", "10.22.241.233:2181");
 * client.set("key", "value"); System.out.println(client.get("key"));
 * 
 * </p>
 * 
 * @author lei.gao
 */
public class RedisClusterPoolClient {
	private static final Log logger = LogFactory
			.getLog(RedisClusterPoolClient.class);
	/**
	 * Number of hash slot(node).
	 */
	private IHashFunc hashFunc;
	private String clusterName;
	private String zkHost;
	private RedisClusterPoolProvider poolsObj;
	public static final int DEFAULT_ZK_TIMEOUT = 2000;
	public static final int DEFAULT_REDIS_TIMEOUT = 3000;
	private int zkTimeout = DEFAULT_ZK_TIMEOUT;
	private int redisTimeout = DEFAULT_REDIS_TIMEOUT;

	private int partsLen;

	/**
	 * RedisClusterPoolClient的默认构造函数，可以满足大多数场景
	 * 
	 * @param clusterName
	 *            业务名称
	 * @param zkHost
	 *            zookeeper地址
	 */
	public RedisClusterPoolClient(String clusterName, String zkHost) {
		this.clusterName = clusterName;
		this.zkHost = zkHost;
	}

	/**
	 * RedisClusterPoolClient的构造函数，如果你不确定请不要使用
	 * 
	 * @param clusterName
	 * @param zkHost
	 * @param poolMaxActive
	 * @param poolMaxIdle
	 * @param poolMaxWait
	 *            maxActive: 1000 the maximum number of objects that can be
	 *            allocated by the pool (checked out to clients, or idle
	 *            awaiting checkout) at a given time. When non-positive, there
	 *            is no limit to the number of objects that can be managed by
	 *            the pool at one time. When maxActive is reached, the pool is
	 *            said to be exhausted. The default setting for this parameter
	 *            is 8. maxIdle: 100 the maximum number of objects that can sit
	 *            idle in the pool at any time. When negative, there is no limit
	 *            to the number of objects that may be idle at one time. The
	 *            default setting for this parameter is 8.
	 */
	public RedisClusterPoolClient(String clusterName, String zkHost,
			int poolMaxActive, int poolMaxIdle, int poolMaxWait) {
		this.clusterName = clusterName;
		this.zkHost = zkHost;
		RedisClusterPoolProvider.setJedisPoolConfig(poolMaxActive, poolMaxIdle,
				poolMaxWait);
	}

	/**
	 * RedisClusterPoolClient的构造函数
	 * 
	 * @param clusterName
	 * @param zkHost
	 * @param redisTimeout
	 *            设置redis超时时间，单位:ms
	 */
	public RedisClusterPoolClient(String clusterName, String zkHost,
			int redisTimeout) {
		this.clusterName = clusterName;
		this.zkHost = zkHost;
		this.redisTimeout = redisTimeout;
	}

	/**
	 * 初始化过程
	 */
	public void init() {
		this.poolsObj = new RedisClusterPoolProvider(clusterName, zkHost,
				zkTimeout, redisTimeout);
		this.hashFunc = poolsObj.getHashFunc();
		this.partsLen = this.poolsObj.getPoolsLen();
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
	 * 设置key的值为value，永久性
	 * 
	 * key是string和key是byte[] 不能混用，因为两者的hash值不同
	 * 
	 * @param key
	 * @param value
	 * @return 成功返回 OK
	 */
	public String set(String key, String value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.set(key, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 得到key的值
	 * 
	 * @param key
	 * @return
	 */
	public String get(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.get(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 判断值是否存在
	 * 
	 * @param key
	 * @return
	 */
	public Boolean exists(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Boolean r = jedis.exists(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 返回key的类型,不存在则为none
	 * 
	 * @param key
	 * @return
	 */
	public String type(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.type(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 为 key 设置生存时间，过期时间为seconds秒
	 * 
	 * @param key
	 * @param seconds
	 * @return
	 */
	public Long expire(String key, int seconds) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.expire(key, seconds);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long expire(int seconds, String... keys) {
		try {
			long result = 0;
			for (String key : keys) {
				RedisClusterPool pool = (RedisClusterPool) this.poolsObj
						.getPool(hashFunc.hash(key));
				Jedis jedis = pool.getResource();
				result += jedis.expire(key, seconds);
				pool.returnResource(jedis);
			}
			return result;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long expireBinary(final Set<byte[]> keys, int seconds) {
		try {
			long result = 0;
			for (byte[] key : keys) {
				RedisClusterPool pool = (RedisClusterPool) this.poolsObj
						.getPool(hashFunc.hash(key));
				Jedis jedis = pool.getResource();
				result += jedis.expire(key, seconds);
				pool.returnResource(jedis);
			}
			return result;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * EXPIREAT 命令接受的时间参数是 UNIX 时间戳
	 * 
	 * 例如:EXPIREAT cache 1355292000 # 这个 key 将在 2012.12.12 过期
	 * 
	 * @param key
	 * @param unixTime
	 * @return
	 */
	public Long expireAt(String key, long unixTime) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.expireAt(key, unixTime);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}

	}

	/**
	 * 返回生存时间 如果是永久性则返回-1
	 * 
	 * @param key
	 * @return
	 */
	public Long ttl(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.ttl(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}

	}

	/**
	 * 设置或者清空key的value(字符串)在offset处的bit值
	 * 
	 * http://redis.cn/commands/setbit.html
	 * 
	 * @param key
	 * @param offset
	 * @param value
	 * @return
	 */
	public boolean setbit(String key, long offset, boolean value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			boolean r = jedis.setbit(key, offset, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}

	}

	/**
	 * 返回位的值存储在关键的字符串值的偏移量。
	 * 
	 * @param key
	 * @param offset
	 * @return
	 */
	public boolean getbit(String key, long offset) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			boolean r = jedis.getbit(key, offset);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}

	}

	/**
	 * 覆盖key对应的string的一部分，从指定的offset处开始，覆盖value的长度
	 * 
	 * @param key
	 * @param offset
	 * @param value
	 * @return
	 */
	public long setrange(String key, long offset, String value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			long r = jedis.setrange(key, offset, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}

	}

	/**
	 * 获取存储在key上的值的一个子字符串
	 * 
	 * @param key
	 * @param startOffset
	 * @param endOffset
	 * @return
	 */
	public String getrange(String key, long startOffset, long endOffset) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.getrange(key, startOffset, endOffset);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}

	}

	/**
	 * 自动将key对应到value并且返回原来key对应的value
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public String getSet(String key, String value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.getSet(key, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}

	}

	/**
	 * 如果key不存在，就设置key对应字符串value。在这种情况下，该命令和SET一样。当key已经存在时，就不做任何操作。SETNX是
	 * "SET if Not eXists"。
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public Long setnx(String key, String value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.setnx(key, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}

	}

	/**
	 * 设置key-value并设置过期时间（单位：秒）
	 * 
	 * @param key
	 * @param seconds
	 * @param value
	 * @return
	 */
	public String setex(String key, int seconds, String value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.setex(key, seconds, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}

	}

	/**
	 * 原子减指定的整数
	 * 
	 * @param key
	 * @param integer
	 * @return
	 */
	public Long decrBy(String key, long integer) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.decrBy(key, integer);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}

	}

	/**
	 * 整数原子减1
	 * 
	 * @param key
	 * @return
	 */
	public Long decr(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.decr(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}

	}

	/**
	 * 执行原子增加一个整数
	 * 
	 * @param key
	 * @param integer
	 * @return
	 */
	public Long incrBy(String key, long integer) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.incrBy(key, integer);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}

	}

	/**
	 * 执行原子加1操作
	 * 
	 * @param key
	 * @return
	 */
	public Long incr(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.incr(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}

	}

	/**
	 * 追加一个值到key上
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public Long append(String key, String value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.append(key, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}

	}

	/**
	 * 设置hash里面一个字段的值
	 * 
	 * @param key
	 * @param field
	 * @param value
	 * @return
	 */
	public Long hset(String key, String field, String value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.hset(key, field, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 读取哈希域的的值
	 * 
	 * @param key
	 * @param field
	 * @return
	 */
	public String hget(String key, String field) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.hget(key, field);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 设置hash的一个字段，只有当这个字段不存在时有效
	 * 
	 * @param key
	 * @param field
	 * @param value
	 * @return
	 */
	public Long hsetnx(String key, String field, String value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.hsetnx(key, field, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 设置hash字段值
	 * 
	 * @param key
	 * @param hash
	 * @return
	 */
	public String hmset(String key, Map<String, String> hash) {
		if (hash == null || hash.isEmpty())
			throw new ClusterOpException("Param cannot be null or empty!");
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.hmset(key, hash);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 获取hash里面指定字段的值
	 * 
	 * @param key
	 * @param fields
	 * @return
	 */
	public List<String> hmget(String key, String... fields) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			List<String> r = jedis.hmget(key, fields);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 将哈希集中指定域的值增加给定的数字
	 * 
	 * @param key
	 * @param field
	 * @param value
	 * @return
	 */
	public Long hincrBy(String key, String field, long value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.hincrBy(key, field, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 判断给定域是否存在于哈希集中
	 * 
	 * @param key
	 * @param field
	 * @return
	 */
	public Boolean hexists(String key, String field) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Boolean r = jedis.hexists(key, field);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 删除一个或多个哈希域
	 * 
	 * @param key
	 * @param fields
	 * @return
	 */
	public Long hdel(String key, String... fields) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.hdel(key, fields);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 获取hash里所有字段的数量
	 * 
	 * @param key
	 * @return
	 */
	public Long hlen(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.hlen(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 获取hash的所有字段
	 * 
	 * @param key
	 * @return
	 */
	public Set<String> hkeys(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<String> r = jedis.hkeys(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 获得hash的所有值
	 * 
	 * @param key
	 * @return
	 */
	public List<String> hvals(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			List<String> r = jedis.hvals(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 从哈希集中读取全部的域和值
	 * 
	 * @param key
	 * @return
	 */
	public Map<String, String> hgetAll(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Map<String, String> r = jedis.hgetAll(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 从队列的右边入队一个元素
	 * 
	 * @param key
	 * @param strings
	 * @return
	 */
	public Long rpush(String key, String... strings) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.rpush(key, strings);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 从队列的左边入队一个或多个元素
	 * 
	 * @param key
	 * @param strings
	 * @return
	 */
	public Long lpush(String key, String... strings) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.lpush(key, strings);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 获得队列(List)的长度
	 * 
	 * @param key
	 * @return
	 */
	public Long llen(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.llen(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 从列表中获取指定返回的元素
	 * 
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public List<String> lrange(String key, long start, long end) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			List<String> r = jedis.lrange(key, start, end);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 修剪(trim)一个已存在的 list，这样 list 就会只包含指定范围的指定元素。start 和 stop 都是由0开始计数的， 这里的 0
	 * 是列表里的第一个元素（表头），1 是第二个元素，以此类推。 例如： LTRIM foobar 0 2 将会对存储在 foobar
	 * 的列表进行修剪，只保留列表里的前3个元素。 start 和 end 也可以用负数来表示与表尾的偏移量，比如 -1 表示列表里的最后一个元素， -2
	 * 表示倒数第二个，等等。 超过范围的下标并不会产生错误：如果 start 超过列表尾部，或者 start > end，结果会是列表变成空表（即该
	 * key 会被移除）。 如果 end 超过列表尾部，Redis 会将其当作列表的最后一个元素。 LTRIM 的一个常见用法是和 LPUSH /
	 * RPUSH 一起使用
	 * 
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public String ltrim(String key, long start, long end) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.ltrim(key, start, end);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 获取一个元素，通过其索引列表
	 * 
	 * @param key
	 * @param index
	 * @return
	 */
	public String lindex(String key, long index) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.lindex(key, index);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 设置队列里面一个元素的值
	 * 
	 * @param key
	 * @param index
	 * @param value
	 * @return
	 */
	public String lset(String key, long index, String value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.lset(key, index, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 从列表中删除元素
	 * 
	 * 从存于 key 的列表里移除前 count 次出现的值为 value 的元素。 这个 count 参数通过下面几种方式影响这个操作： count
	 * > 0: 从头往尾移除值为 value 的元素。 count < 0: 从尾往头移除值为 value 的元素。 count = 0: 移除所有值为
	 * value 的元素。
	 * 
	 * @param key
	 * @param count
	 * @param value
	 * @return
	 */
	public Long lrem(String key, long count, String value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.lrem(key, count, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 删除，并获得该列表中的第一元素，或阻塞，直到有一个可用
	 * 
	 * BLPOP 是阻塞式列表的弹出原语。 它是命令 LPOP 的阻塞版本，这是因为当给定列表内没有任何元素可供弹出的时候， 连接将被 BLPOP
	 * 命令阻塞。 当给定多个 key 参数时，按参数 key 的先后顺序依次检查各个列表，弹出第一个非空列表的头元素。 非阻塞行为 当 BLPOP
	 * 被调用时，如果给定 key 内至少有一个非空列表，那么弹出遇到的第一个非空列表的头元素，并和被弹出元素所属的列表的名字 key
	 * 一起，组成结果返回给调用者。 当存在多个给定 key 时， BLPOP 按给定 key 参数排列的先后顺序，依次检查各个列表。 我们假设 key
	 * list1 不存在，而 list2 和 list3 都是非空列表
	 * 
	 * @param key
	 * @param timeoutSecs
	 * @return
	 */
	public String blpop(String key, final int timeoutSecs) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			List<String> r = jedis.blpop(timeoutSecs, key);
			pool.returnResource(jedis);
			if (r != null && r.size() == 2) {
				return r.get(1);
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 删除，并获得该列表中的最后一个元素，或阻塞，直到有一个可用
	 * 
	 * @param key
	 * @param timeoutSecs
	 * @return
	 */
	public String brpop(String key, final int timeoutSecs) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			List<String> r = jedis.brpop(timeoutSecs, key);
			pool.returnResource(jedis);
			if (r != null && r.size() == 2) {
				return r.get(1);
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 删除，并获得该列表中的第一元素，或阻塞，直到有一个可用
	 * 
	 * @param key
	 * @param timeoutSecs
	 * @return
	 */
	public byte[] blpop(byte[] key, final int timeoutSecs) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			List<byte[]> r = jedis.blpop(timeoutSecs, key);
			pool.returnResource(jedis);
			if (r != null && r.size() == 2) {
				return r.get(1);
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 删除，并获得该列表中的第一元素，或阻塞，直到有一个可用
	 * 
	 * @param key
	 * @param timeoutSecs
	 * @return
	 */
	public byte[] brpop(byte[] key, final int timeoutSecs) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			List<byte[]> r = jedis.brpop(timeoutSecs, key);
			pool.returnResource(jedis);
			if (r != null && r.size() == 2) {
				return r.get(1);
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 从队列的左边出队一个元素
	 * 
	 * @param key
	 * @return
	 */
	public String lpop(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.lpop(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 从队列的右边出队一个元素
	 * 
	 * @param key
	 * @return
	 */
	public String rpop(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.rpop(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 添加一个或者多个元素到集合(set)里
	 * 
	 * @param key
	 * @param members
	 * @return
	 */
	public Long sadd(String key, String... members) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.sadd(key, members);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 获取集合里面的所有key
	 * 
	 * @param key
	 * @return
	 */
	public Set<String> smembers(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<String> r = jedis.smembers(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 从集合里删除一个或多个key
	 * 
	 * @param key
	 * @param members
	 * @return
	 */
	public Long srem(String key, String... members) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.srem(key, members);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 移除并返回一个集合中的随机元素
	 * 
	 * @param key
	 * @return
	 */
	public String spop(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.spop(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 获取集合里面的元素数量
	 * 
	 * @param key
	 * @return
	 */
	public Long scard(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.scard(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 确定一个给定的值是一个集合的成员
	 * 
	 * @param key
	 * @param member
	 * @return
	 */
	public Boolean sismember(String key, String member) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Boolean r = jedis.sismember(key, member);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 从集合里面随机获取一个key
	 * 
	 * @param key
	 * @return
	 */
	public String srandmember(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.srandmember(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 添加到有序set的一个或多个成员，或更新的分数，如果它已经存在
	 * 
	 * @param key
	 * @param score
	 * @param member
	 * @return
	 */
	public Long zadd(String key, double score, String member) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.zadd(key, score, member);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 返回有序集key中，指定区间内的成员。其中成员按score值递增(从小到大)来排序。具有相同score值的成员按字典序来排列。
	 * 
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public Set<String> zrange(String key, int start, int end) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<String> r = jedis.zrange(key, start, end);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 从排序的集合中删除一个或多个成员
	 * 
	 * @param key
	 * @param members
	 * @return
	 */
	public Long zrem(String key, String... members) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.zrem(key, members);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 增量的一名成员在排序设置的评分
	 * 
	 * @param key
	 * @param score
	 * @param member
	 * @return
	 */
	public Double zincrby(String key, double score, String member) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Double r = jedis.zincrby(key, score, member);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 确定在排序集合成员的索引
	 * 
	 * @param key
	 * @param member
	 * @return
	 */
	public Long zrank(String key, String member) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.zrank(key, member);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 确定指数在排序集的成员，下令从分数高到低
	 * 
	 * @param key
	 * @param member
	 * @return
	 */
	public Long zrevrank(String key, String member) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.zrevrank(key, member);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 
	 * 返回有序集key中，指定区间内的成员。其中成员的位置按score值递减(从大到小)来排列
	 * 
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public Set<String> zrevrange(String key, int start, int end) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<String> r = jedis.zrevrange(key, start, end);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<Tuple> zrangeWithScores(String key, int start, int end) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<Tuple> r = jedis.zrangeWithScores(key, start, end);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<Tuple> zrevrangeWithScores(String key, int start, int end) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<Tuple> r = jedis.zrevrangeWithScores(key, start, end);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 获取一个排序的集合中的成员数量
	 * 
	 * @param key
	 * @return
	 */
	public Long zcard(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.zcard(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 获取成员在排序设置相关的比分
	 * 
	 * @param key
	 * @param member
	 * @return
	 */
	public Double zscore(String key, String member) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Double r = jedis.zscore(key, member);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 对队列、集合、有序集合排序
	 * 
	 * @param key
	 * @return
	 */
	public List<String> sort(String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			List<String> r = jedis.sort(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 带参数排序 <a>http://blog.csdn.net/yousite1/article/details/8486218</a>
	 * 
	 * @param key
	 * @param sortingParameters
	 * @return
	 */
	public List<String> sort(String key, SortingParams sortingParameters) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			List<String> r = jedis.sort(key, sortingParameters);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 返回有序集key中，score值在min和max之间(默认包括score值等于min或max)的成员
	 * 
	 * @param key
	 * @param min
	 * @param max
	 * @return
	 */
	public Long zcount(String key, double min, double max) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.zcount(key, min, max);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 返回key的有序集合中的分数在min和max之间的所有元素（包括分数等于max或者min的元素）。元素被认为是从低分到高分排序的。
	 * 具有相同分数的元素按字典序排列（这个根据redis对有序集合实现的情况而定，并不需要进一步计算）。
	 * 
	 * @param key
	 * @param min
	 * @param max
	 * @return
	 */
	public Set<String> zrangeByScore(String key, double min, double max) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<String> r = jedis.zrangeByScore(key, min, max);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<String> zrevrangeByScore(String key, double max, double min) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<String> r = jedis.zrevrangeByScore(key, max, min);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<String> zrangeByScore(String key, double min, double max,
			int offset, int count) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<String> r = jedis.zrangeByScore(key, min, max, offset, count);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<String> zrevrangeByScore(String key, double max, double min,
			int offset, int count) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<String> r = jedis
					.zrevrangeByScore(key, max, min, offset, count);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<Tuple> r = jedis.zrangeByScoreWithScores(key, min, max);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<Tuple> zrevrangeByScoreWithScores(String key, double max,
			double min) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<Tuple> r = jedis.zrevrangeByScoreWithScores(key, max, min);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<Tuple> zrangeByScoreWithScores(String key, double min,
			double max, int offset, int count) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<Tuple> r = jedis.zrangeByScoreWithScores(key, min, max, offset,
					count);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<Tuple> zrevrangeByScoreWithScores(String key, double max,
			double min, int offset, int count) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<Tuple> r = jedis.zrevrangeByScoreWithScores(key, max, min,
					offset, count);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long zremrangeByRank(String key, int start, int end) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.zremrangeByRank(key, start, end);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long zremrangeByScore(String key, double start, double end) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.zremrangeByScore(key, start, end);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long linsert(String key, LIST_POSITION where, String pivot,
			String value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.linsert(key, where, pivot, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public String set(byte[] key, byte[] value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.set(key, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public byte[] get(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			byte[] r = jedis.get(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Boolean exists(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Boolean r = jedis.exists(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public String type(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.type(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long expire(byte[] key, int seconds) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.expire(key, seconds);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long expire(int seconds, byte[]... keys) {
		try {
			long result = 0;
			for (byte[] key : keys) {
				RedisClusterPool pool = (RedisClusterPool) this.poolsObj
						.getPool(hashFunc.hash(key));
				Jedis jedis = pool.getResource();
				result += jedis.expire(key, seconds);
				pool.returnResource(jedis);
			}
			return result;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long expireAt(byte[] key, long unixTime) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.expireAt(key, unixTime);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long ttl(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.ttl(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public byte[] getSet(byte[] key, byte[] value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			byte[] r = jedis.getSet(key, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long setnx(byte[] key, byte[] value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.setnx(key, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public String setex(byte[] key, int seconds, byte[] value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.setex(key, seconds, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long decrBy(byte[] key, long integer) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.decrBy(key, integer);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long decr(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.decr(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long incrBy(byte[] key, long integer) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.incrBy(key, integer);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long incr(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.incr(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long append(byte[] key, byte[] value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.append(key, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public byte[] substr(byte[] key, int start, int end) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			byte[] r = jedis.substr(key, start, end);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long hset(byte[] key, byte[] field, byte[] value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.hset(key, field, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public byte[] hget(byte[] key, byte[] field) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			byte[] r = jedis.hget(key, field);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long hsetnx(byte[] key, byte[] field, byte[] value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.hsetnx(key, field, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public String hmset(byte[] key, Map<byte[], byte[]> hash) {
		if (hash == null || hash.isEmpty())
			throw new ClusterOpException("Param cannot be null or empty!");
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.hmset(key, hash);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public List<byte[]> hmget(byte[] key, byte[]... fields) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			List<byte[]> r = jedis.hmget(key, fields);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long hincrBy(byte[] key, byte[] field, long value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.hincrBy(key, field, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Boolean hexists(byte[] key, byte[] field) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Boolean r = jedis.hexists(key, field);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long hdel(byte[] key, byte[]... fields) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.hdel(key, fields);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long hlen(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.hlen(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<byte[]> hkeys(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<byte[]> r = jedis.hkeys(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Collection<byte[]> hvals(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Collection<byte[]> r = jedis.hvals(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Map<byte[], byte[]> hgetAll(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Map<byte[], byte[]> r = jedis.hgetAll(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long rpush(byte[] key, byte[]... strings) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.rpush(key, strings);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long lpush(byte[] key, byte[]... strings) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.lpush(key, strings);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long llen(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.llen(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public List<byte[]> lrange(byte[] key, int start, int end) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			List<byte[]> r = jedis.lrange(key, start, end);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public String ltrim(byte[] key, int start, int end) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.ltrim(key, start, end);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public byte[] lindex(byte[] key, int index) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			byte[] r = jedis.lindex(key, index);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public String lset(byte[] key, int index, byte[] value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			String r = jedis.lset(key, index, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long lrem(byte[] key, int count, byte[] value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.lrem(key, count, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public byte[] lpop(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			byte[] r = jedis.lpop(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public byte[] rpop(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			byte[] r = jedis.rpop(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long sadd(byte[] key, byte[]... members) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.sadd(key, members);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<byte[]> smembers(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<byte[]> r = jedis.smembers(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long srem(byte[] key, byte[]... members) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.srem(key, members);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public byte[] spop(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			byte[] r = jedis.spop(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long scard(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.scard(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Boolean sismember(byte[] key, byte[] member) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Boolean r = jedis.sismember(key, member);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public byte[] srandmember(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			byte[] r = jedis.srandmember(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long zadd(byte[] key, double score, byte[] member) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.zadd(key, score, member);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<byte[]> zrange(byte[] key, int start, int end) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<byte[]> r = jedis.zrange(key, start, end);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long zrem(byte[] key, byte[]... members) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.zrem(key, members);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Double zincrby(byte[] key, double score, byte[] member) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Double r = jedis.zincrby(key, score, member);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long zrank(byte[] key, byte[] member) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.zrank(key, member);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long zrevrank(byte[] key, byte[] member) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.zrevrank(key, member);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<byte[]> zrevrange(byte[] key, int start, int end) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<byte[]> r = jedis.zrevrange(key, start, end);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<Tuple> zrangeWithScores(byte[] key, int start, int end) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<Tuple> r = jedis.zrangeWithScores(key, start, end);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<Tuple> zrevrangeWithScores(byte[] key, int start, int end) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<Tuple> r = jedis.zrevrangeWithScores(key, start, end);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long zcard(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.zcard(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Double zscore(byte[] key, byte[] member) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Double r = jedis.zscore(key, member);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public List<byte[]> sort(byte[] key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			List<byte[]> r = jedis.sort(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			List<byte[]> r = jedis.sort(key, sortingParameters);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long zcount(byte[] key, double min, double max) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.zcount(key, min, max);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<byte[]> r = jedis.zrangeByScore(key, min, max);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<byte[]> zrangeByScore(byte[] key, double min, double max,
			int offset, int count) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<byte[]> r = jedis.zrangeByScore(key, min, max, offset, count);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<Tuple> r = jedis.zrangeByScoreWithScores(key, min, max);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min,
			double max, int offset, int count) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<Tuple> r = jedis.zrangeByScoreWithScores(key, min, max, offset,
					count);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<byte[]> r = jedis.zrevrangeByScore(key, max, min);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min,
			int offset, int count) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<byte[]> r = jedis
					.zrevrangeByScore(key, max, min, offset, count);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max,
			double min) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<Tuple> r = jedis.zrevrangeByScoreWithScores(key, max, min);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max,
			double min, int offset, int count) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Set<Tuple> r = jedis.zrevrangeByScoreWithScores(key, max, min,
					offset, count);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long zremrangeByRank(byte[] key, int start, int end) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.zremrangeByRank(key, start, end);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long zremrangeByScore(byte[] key, double start, double end) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.zremrangeByScore(key, start, end);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long linsert(byte[] key, LIST_POSITION where, byte[] pivot,
			byte[] value) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.linsert(key, where, pivot, value);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long del(final String key) {
		try {
			RedisClusterPool pool = (RedisClusterPool) this.poolsObj
					.getPool(hashFunc.hash(key));
			Jedis jedis = pool.getResource();
			Long r = jedis.del(key);
			pool.returnResource(jedis);
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long del(final String... keys) {

		try {
			Map<Integer, List<String>> mkeys = new HashMap<Integer, List<String>>();
			for (String key : keys) {
				int part = hashFunc.hash(key);
				List<String> mlist = new ArrayList<String>();
				if (mkeys.containsKey(part)) {
					mlist = mkeys.get(part);
				}
				mlist.add(key);
				mkeys.put(part, mlist);
			}
			Long r = 0l;
			for (int part : mkeys.keySet()) {
				List<String> mlist = mkeys.get(part);
				String[] delKeys = (String[]) mlist.toArray(new String[mlist
						.size()]);
				RedisClusterPool pool = (RedisClusterPool) this.poolsObj
						.getPool(part);
				Jedis jedis = pool.getResource();
				r += jedis.del(delKeys);
				pool.returnResource(jedis);
			}
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long del(final List<String> keys) {
		String[] keyss = keys.toArray(new String[keys.size()]);
		return del(keyss);
	}

	public Long delBinary(final List<byte[]> keys) {
		byte[][] delKeys = (byte[][]) keys.toArray(new byte[keys.size()][]);
		return del(delKeys);
	}

	public Long del(final byte[]... keys) {
		try {
			Map<Integer, List<byte[]>> mkeys = new HashMap<Integer, List<byte[]>>();
			for (byte[] key : keys) {
				int part = hashFunc.hash(key);
				List<byte[]> mlist = new ArrayList<byte[]>();
				if (mkeys.containsKey(part)) {
					mlist = mkeys.get(part);
				}
				mlist.add(key);
				mkeys.put(part, mlist);
			}
			Long r = 0l;
			for (int part : mkeys.keySet()) {
				List<byte[]> mlist = mkeys.get(part);
				byte[][] delKeys = (byte[][]) mlist.toArray(new byte[mlist
						.size()][]);
				RedisClusterPool pool = (RedisClusterPool) this.poolsObj
						.getPool(part);
				Jedis jedis = pool.getResource();
				r += jedis.del(delKeys);
				pool.returnResource(jedis);
			}
			return r;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public List<String> mget(final List<String> keys) {
		String[] keyss = keys.toArray(new String[keys.size()]);
		return mget(keyss);
	}

	public List<String> mget(final String... keys) {
		try {
			List<String> ret = new ArrayList<String>(keys.length);
			for (int g = 0; g < keys.length; g++) {
				ret.add(null);
			}
			int[][] shadow = new int[partsLen][keys.length];
			int[] count = new int[partsLen];
			List<List<String>> mkeys = new ArrayList<List<String>>(partsLen);
			List<List<String>> mvals = new ArrayList<List<String>>(partsLen);
			for (int h = 0; h < partsLen; h++) {
				mkeys.add(new ArrayList<String>(keys.length));
				mvals.add(new ArrayList<String>(keys.length));
			}
			for (int j = 0; j < keys.length; j++) {
				int part = hashFunc.hash(keys[j]);
				mkeys.get(part).add(keys[j]);
				shadow[part][count[part]] = j;
				count[part]++;
			}
			for (int i = 0; i < partsLen; i++) {
				if (!mkeys.get(i).isEmpty()) {
					RedisClusterPool pool = (RedisClusterPool) this.poolsObj
							.getPool(i);
					Jedis jedis = pool.getResource();
					String[] mgkeys = mkeys.get(i).toArray(
							new String[mkeys.get(i).size()]);
					mvals.set(i, jedis.mget(mgkeys));
					pool.returnResource(jedis);
				}
			}
			for (int k = 0; k < partsLen; k++) {
				for (int m = 0; m < count[k]; m++) {
					ret.set(shadow[k][m], mvals.get(k).get(m));
				}
			}
			return ret;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public List<byte[]> mgetBinary(final List<byte[]> keys) {
		byte[][] mgkeys = (byte[][]) keys.toArray(new byte[keys.size()][]);
		return mget(mgkeys);
	}

	public List<byte[]> mget(final byte[]... keys) {
		try {
			List<byte[]> ret = new ArrayList<byte[]>(keys.length);
			for (int g = 0; g < keys.length; g++) {
				ret.add(null);
			}
			int[][] shadow = new int[partsLen][keys.length];
			int[] count = new int[partsLen];
			List<List<byte[]>> mkeys = new ArrayList<List<byte[]>>(partsLen);
			List<List<byte[]>> mvals = new ArrayList<List<byte[]>>(partsLen);
			for (int h = 0; h < partsLen; h++) {
				mkeys.add(new ArrayList<byte[]>(keys.length));
				mvals.add(new ArrayList<byte[]>(keys.length));
			}
			for (int j = 0; j < keys.length; j++) {
				int part = hashFunc.hash(keys[j]);
				mkeys.get(part).add(keys[j]);
				shadow[part][count[part]] = j;
				count[part]++;
			}
			for (int i = 0; i < partsLen; i++) {
				if (!mkeys.get(i).isEmpty()) {
					RedisClusterPool pool = (RedisClusterPool) this.poolsObj
							.getPool(i);
					Jedis jedis = pool.getResource();
					byte[][] mgkeys = (byte[][]) mkeys.get(i).toArray(
							new byte[mkeys.get(i).size()][]);
					mvals.set(i, jedis.mget(mgkeys));
					pool.returnResource(jedis);
				}
			}
			for (int k = 0; k < partsLen; k++) {
				for (int m = 0; m < count[k]; m++) {
					ret.set(shadow[k][m], mvals.get(k).get(m));
				}
			}
			return ret;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long mset(final Map<String, String> keysvalues) {
		try {
			Long ret = 0L;
			List<Map<String, String>> mkeysvalues = new ArrayList<Map<String, String>>(
					partsLen);
			for (int h = 0; h < partsLen; h++) {
				mkeysvalues.add(new HashMap<String, String>(keysvalues.size()));
			}
			for (Entry<String, String> keyvalue : keysvalues.entrySet()) {
				int part = hashFunc.hash(keyvalue.getKey());
				mkeysvalues.get(part).put(keyvalue.getKey(),
						keyvalue.getValue());
			}
			for (int i = 0; i < partsLen; i++) {
				Map<String, String> mkeyvalue = mkeysvalues.get(i);
				if (!mkeyvalue.isEmpty()) {
					String[] mskeyvalue = new String[mkeyvalue.size() * 2];
					int ct = -1;
					for (String key : mkeyvalue.keySet()) {
						mskeyvalue[++ct] = key;
						mskeyvalue[++ct] = mkeyvalue.get(key);
					}

					RedisClusterPool pool = (RedisClusterPool) this.poolsObj
							.getPool(i);
					Jedis jedis = pool.getResource();
					ret += jedis.mset(mskeyvalue).equals("OK") ? mkeyvalue
							.size() : 0;
					pool.returnResource(jedis);
				}
			}
			return ret;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long msetnx(final Map<String, String> keysvalues) {
		try {
			Long ret = 0L;
			List<Map<String, String>> mkeysvalues = new ArrayList<Map<String, String>>(
					partsLen);
			for (int h = 0; h < partsLen; h++) {
				mkeysvalues.add(new HashMap<String, String>(keysvalues.size()));
			}
			for (Entry<String, String> keyvalue : keysvalues.entrySet()) {
				int part = hashFunc.hash(keyvalue.getKey());
				mkeysvalues.get(part).put(keyvalue.getKey(),
						keyvalue.getValue());
			}
			for (int i = 0; i < partsLen; i++) {
				Map<String, String> mkeyvalue = mkeysvalues.get(i);
				if (!mkeyvalue.isEmpty()) {
					String[] mskeyvalue = new String[mkeyvalue.size() * 2];
					int ct = -1;
					for (String key : mkeyvalue.keySet()) {
						mskeyvalue[++ct] = key;
						mskeyvalue[++ct] = mkeyvalue.get(key);
					}

					RedisClusterPool pool = (RedisClusterPool) this.poolsObj
							.getPool(i);
					Jedis jedis = pool.getResource();
					ret += jedis.msetnx(mskeyvalue).equals("OK") ? mkeyvalue
							.size() : 0;
					pool.returnResource(jedis);
				}
			}
			return ret;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	public Long msetBinary(final Map<byte[], byte[]> keysvalues) {
		try {
			Long ret = 0L;
			List<Map<byte[], byte[]>> mkeysvalues = new ArrayList<Map<byte[], byte[]>>(
					partsLen);
			for (int h = 0; h < partsLen; h++) {
				mkeysvalues.add(new HashMap<byte[], byte[]>(keysvalues.size()));
			}
			for (Entry<byte[], byte[]> keyvalue : keysvalues.entrySet()) {
				int part = hashFunc.hash(keyvalue.getKey());
				mkeysvalues.get(part).put(keyvalue.getKey(),
						keyvalue.getValue());
			}
			for (int i = 0; i < partsLen; i++) {
				Map<byte[], byte[]> mkeyvalue = mkeysvalues.get(i);
				if (!mkeyvalue.isEmpty()) {
					byte[][] mskeyvalue = new byte[mkeyvalue.size() * 2][];
					int ct = -1;
					for (byte[] key : mkeyvalue.keySet()) {
						mskeyvalue[++ct] = key;
						mskeyvalue[++ct] = mkeyvalue.get(key);
					}

					RedisClusterPool pool = (RedisClusterPool) this.poolsObj
							.getPool(i);
					Jedis jedis = pool.getResource();
					ret += jedis.mset(mskeyvalue).equals("OK") ? mkeyvalue
							.size() : 0;
					pool.returnResource(jedis);
				}
			}
			return ret;
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

	/**
	 * 清空整个 Redis 服务器的数据(删除所有数据库的所有 key )。
	 */
	public void flushAll() {
		try {
			for (int i = 0; i < partsLen; i++) {
				RedisClusterPool pool = (RedisClusterPool) this.poolsObj
						.getPool(i);
				Jedis jedis = pool.getResource();
				jedis.flushAll();
				pool.returnResource(jedis);
			}
		} catch (Exception e) {
			throw new ClusterOpException(e);
		}
	}

}
