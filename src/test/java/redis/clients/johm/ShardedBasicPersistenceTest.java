package redis.clients.johm;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.shardedcluster.ShardedJedisCluster;

public class ShardedBasicPersistenceTest extends BasicPersistenceTest {

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
