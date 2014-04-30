package redis.clients.johm;

import static org.junit.Assert.*;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.shardedcluster.ShardedJedisCluster;

public class ShardedSearchTest extends SearchTest{

	ShardedJedisPool jedisPool = null;
	
	@Before
  public void startUp() {
      startJedisEngine();
  }
	protected void startJedisEngine() {
		Boolean isSharded = true;
    String clusterConfigFile= System.getProperty("jedis.cluster.config");
    ShardedJedisCluster.bootstrap(clusterConfigFile);
    JOhm.setPool(ShardedJedisCluster.getPool());
    purgeRedis();
}
	 protected void purgeRedis() {
		 
		 jedisPool = ShardedJedisCluster.getPool();
		 ShardedJedis jedis = jedisPool.getResource();
     jedis.flushAll();
     jedisPool.returnResource(jedis);
 }
	
}
