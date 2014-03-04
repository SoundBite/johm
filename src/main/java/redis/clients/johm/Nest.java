package redis.clients.johm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.TransactionBlock;
import redis.clients.jedis.ZParams;

public class Nest<T> {
    private static final String COLON = ":";
    private StringBuilder sb;
    private String key;
    private ArrayList<String> keys;
    private JedisPool jedisPool;

    public void setJedisPool(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
        checkRedisLiveness();
    }

    public Nest<T> fork() {
        return new Nest<T>(key());
    }

    public Nest() {
        this.key = "";
    }

    public Nest(String key) {
        this.key = key;
    }

    public Nest(Class<T> clazz) {
        this.key = clazz.getSimpleName();
    }

    public Nest(T model) {
        this.key = model.getClass().getSimpleName();
    }

    public String key() {
        prefix();
        String generatedKey = sb.toString();
        generatedKey = generatedKey.substring(0, generatedKey.length() - 1);
        sb = null;
        return generatedKey;
    }
    public List<String> keys() {
        return keys;
    }
    
    public String combineKeys() {
    	if (keys == null){
    		return null;
    	}
    	StringBuilder combinedKey = new StringBuilder();
    	List<String> newListOfKeys = new ArrayList<String>(keys);
    	Collections.sort(newListOfKeys);
    	for (String key:newListOfKeys) {
    		combinedKey.append(key);
    		combinedKey.append(COLON);
    	}
    	
    	String combinedKeyStr = null;
    	if (combinedKey.length() > 0) {
    		combinedKeyStr = combinedKey.substring(0, combinedKey.lastIndexOf(COLON));
    	}
    	return combinedKeyStr.toString();
    }
    
    private void prefix() {
        if (sb == null) {
            sb = new StringBuilder();
            sb.append(key);
            sb.append(COLON);
        }
    }

    public Nest<T> cat(int id) {
        prefix();
        sb.append(id);
        sb.append(COLON);
        return this;
    }

    public Nest<T> cat(Object field) {
        prefix();
        sb.append(field);
        sb.append(COLON);
        return this;
    }

    public Nest<T> cat(String field) {
        prefix();
        sb.append(field);
        sb.append(COLON);
        return this;
    }
    
    public Nest<T> next() {
        if(keys==null) {
            keys=new java.util.ArrayList<String>();
        }
        keys.add(key());
        return this;
    }
    
    // Redis Common Operations
    public String set(String value) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		String set = jedis.set(key(), value);
    		return set;
    	}finally {
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public String get() {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		String string = jedis.get(key());
    		return string;
    	}finally {
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public Long incr() {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Long incr = jedis.incr(key());
    		return incr;
    	}finally {
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public List<Object> multi(TransactionBlock transaction) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		List<Object> multi = jedis.multi(transaction);
    		return multi;
    	}finally {
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public List<Object> multiWithWatch(TransactionBlock transaction, String...keys) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		jedis.watch(keys);
    		List<Object> multi = jedis.multi(transaction);
    		return multi;
    	}finally {
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public Long del() {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Long del = jedis.del(key());
    		return del;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public Boolean exists() {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Boolean exists = jedis.exists(key());
    		return exists;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    // Redis Hash Operations
    public String hmset(Map<String, String> hash) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		String hmset = jedis.hmset(key(), hash);
    		return hmset;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public Map<String, String> hgetAll() {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Map<String, String> hgetAll = jedis.hgetAll(key());
    		return hgetAll;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public String hget(String field) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		String value = jedis.hget(key(), field);
    		return value;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public Long hdel(String field) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Long hdel = jedis.hdel(key(), field);
    		return hdel;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public Long hlen() {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Long hlen = jedis.hlen(key());
    		return hlen;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public Set<String> hkeys() {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Set<String> hkeys = jedis.hkeys(key());
    		return hkeys;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    // Redis Set Operations
    public Long sadd(String member) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Long reply = jedis.sadd(key(), member);
    		return reply;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public Long srem(String member) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Long reply = jedis.srem(key(), member);
    		return reply;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public Set<String> smembers() {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Set<String> members = jedis.smembers(key());
    		return members;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public Set<String> sinter() {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Set<String> members = jedis.sinter((String[])keys.toArray(new String[0]));
    		return members;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public void sinterstore(final String dstkey) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		jedis.sinterstore(dstkey, (String[])keys.toArray(new String[0]));
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    // Redis List Operations
    public Long rpush(String string) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Long rpush = jedis.rpush(key(), string);
    		return rpush;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public String lset(int index, String value) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		String lset = jedis.lset(key(), index, value);
    		return lset;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public String lindex(int index) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		String lindex = jedis.lindex(key(), index);
    		return lindex;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public Long llen() {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Long llen = jedis.llen(key());
    		return llen;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public Long lrem(int count, String value) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Long lrem = jedis.lrem(key(), count, value);
    		return lrem;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public List<String> lrange(int start, int end) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		List<String> lrange = jedis.lrange(key(), start, end);
    		return lrange;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    // Redis SortedSet Operations
    public Set<String> zrange(int start, int end) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Set<String> zrange = jedis.zrange(key(), start, end);
    		return zrange;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }
    
    public Set<String> zrangebyscore(double min, double max) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Set<String> zrange = jedis.zrangeByScore(key(), min, max);
    		return zrange;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }
    
    public Set<String> zrangebyscore(String min, String max) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Set<String> zrange = jedis.zrangeByScore(key(), min, max);
    		return zrange;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public Long zadd(float score, String member) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Long zadd = jedis.zadd(key(), score, member);
    		return zadd;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }
    
    public Long zadd(double score, String member) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Long zadd = jedis.zadd(key(), score, member);
    		return zadd;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public void zinterstore(final String dstkey, ZParams params){
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		jedis.zinterstore(dstkey, params, (String[])keys.toArray(new String[0]));
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public Long zrem(String member) {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Long zrem = jedis.zrem(key(), member);
    		return zrem;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public Long zcard() {
    	Jedis jedis = null;
    	try{
    		jedis = getResource();
    		Long zadd = jedis.zcard(key());
    		return zadd;
    	}finally{
    		if (jedis != null) returnResource(jedis);
    	}
    }

    public Pipeline pipelined(final Jedis jedis) {
    	return jedis.pipelined();
    }
    
    public void returnResource(final Jedis jedis) {
        jedisPool.returnResource(jedis);
    }

    public Jedis getResource() {
        Jedis jedis;
        jedis = jedisPool.getResource();
        return jedis;
    }

    private void checkRedisLiveness() {
        if (jedisPool == null) {
            throw new JOhmException(
                    "JOhm will fail to do most useful tasks without Redis",
                    JOhmExceptionMeta.NULL_JEDIS_POOL);
        }
    }
}
