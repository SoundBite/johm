package redis.clients.johm;


import org.junit.Before;
import org.junit.Ignore;


import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.shardedcluster.ShardedJedisCluster;

@Ignore
public class ShardedSearchTest extends SearchTest{

	ShardedJedisPool jedisPool = null;
	
	@Before
  public void startUp() {
      startJedisEngine();
  }
	protected void startJedisEngine() {
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
