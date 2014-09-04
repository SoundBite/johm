package redis.clients.johm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBlock;
import redis.clients.jedis.ZParams;
import redis.clients.johm.JOhmException;
import redis.clients.johm.JOhmExceptionMeta;

public class Nest<T> {
    private static final String COLON = ":";
    private StringBuilder sb;
    private String key;
    private ArrayList<String> keys;
    private JedisPool jedisPool;
    private ShardedJedisPool shardedJedisPool;
    private boolean isSharded;
    

    public void setJedisPool(Object pool, boolean isSharded) {
    	this.isSharded = isSharded;
    	if(isSharded){
    		shardedJedisPool = (ShardedJedisPool) pool;
    	}else{
    		jedisPool = (JedisPool) pool;
    	}
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
		Boolean ex =false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				String set = jedis.set(key(), value);
				return set;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    }finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				String set = jedis.set(key(), value);
				return set;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    }finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}
    	
	public String get() {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				String string = jedis.get(key());
				return string;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				String string = jedis.get(key());
				return string;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public Long incr() {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Long incr = jedis.incr(key());
				return incr;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Long incr = jedis.incr(key());
				return incr;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	/**
	 * multi for ShardedJedis. It returns Transaction for given keys.
	 * The keys should have the same hashtag
	 * @param keys
	 * @return Transaction
	 */
	public Transaction multi(ShardedJedis sJedis, String... keys) {
		Jedis jedis = sJedis.getShard(keys[0]);
		jedis.watch(keys);
		Transaction t = jedis.multi();
		return t;
	}
	
	/**
	 * multi for ShardedJedis. It returns Transaction for a given hashTag.
	 *
	 * @param hashTag
	 * @return Transaction
	 */
	public Transaction multi(ShardedJedis sJedis, String hashTag) {
		Jedis jedis = sJedis.getShard(hashTag);
		Transaction t = jedis.multi();
		return t;
	} 
	
	
    public List<Object> multi(TransactionBlock transaction) {
    	Jedis jedis = null;
    	Boolean ex = false;
    	List<Object> multi = null;
    	try{
    		jedis = getResource();
    		multi = jedis.multi(transaction);
    		return multi;
    	} catch (Exception e) {
			e.printStackTrace();
			ex = true;
			if (jedis != null) {
				returnBrokenResource(jedis);
				throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
			}
			return multi;
	    }finally {
    		if (jedis != null && !ex) {
    			returnResource(jedis);
    		}
    	}
    }

    public List<Object> multiWithWatch(TransactionBlock transaction, String...keys) {
    	Jedis jedis = null;
    	Boolean ex = false;
    	try{
    		jedis = getResource();
    		jedis.watch(keys);
    		List<Object> multi = jedis.multi(transaction);
    		return multi;
    	} catch (Exception e) {
			e.printStackTrace();
			ex = true;
			if (jedis != null) {
				returnBrokenResource(jedis);
				throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
			}
			return null;
	    } finally {
    		if (jedis != null && !ex) {
    			returnResource(jedis);
    		}
    	}
    }

	public Long del() {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Long del = jedis.del(key());
				return del;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {

			Jedis jedis = null;
			try {
				jedis = getResource();
				Long del = jedis.del(key());
				return del;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public Boolean exists() {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Boolean exists = jedis.exists(key());
				return exists;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return false;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {

			Jedis jedis = null;
			try {
				jedis = getResource();
				Boolean exists = jedis.exists(key());
				return exists;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return false;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}
	
	public Boolean exists(String key) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Boolean exists = jedis.exists(key);
				return exists;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return Boolean.FALSE;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {

			Jedis jedis = null;
			try {
				jedis = getResource();
				Boolean exists = jedis.exists(key);
				return exists;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return Boolean.FALSE;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	// Redis Hash Operations
	public String hmset(Map<String, String> hash) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				String hmset = jedis.hmset(key(), hash);
				return hmset;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				String hmset = jedis.hmset(key(), hash);
				return hmset;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}
	
	// Redis Hash Operations
		public String hmset(String key, Map<String, String> hash) {
			Boolean ex = false;
			if (isSharded) {
				ShardedJedis jedis = null;
				try {
					jedis = getShardedResource();
					String hmset = jedis.hmset(key, hash);
					return hmset;
				} catch (Exception e) {
					e.printStackTrace();
					ex = true;
					if (jedis != null) {
						returnBrokenShardedResource(jedis);
						throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
					}
					return null;
			    } finally {
					if (jedis != null && !ex) {
						returnShardedResource(jedis);
					}
				}
			} else {
				Jedis jedis = null;
				try {
					jedis = getResource();
					String hmset = jedis.hmset(key, hash);
					return hmset;
				} catch (Exception e) {
					e.printStackTrace();
					ex = true;
					if (jedis != null) {
						returnBrokenResource(jedis);
						throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
					}
					return null;
			    } finally {
			    	
					if (jedis != null && !ex) {
						returnResource(jedis);
					}
				}
			}
		}
		

	public Map<String, String> hgetAll() {
		Boolean ex =false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Map<String, String> hgetAll = jedis.hgetAll(key());
				return hgetAll;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Map<String, String> hgetAll = jedis.hgetAll(key());
				return hgetAll;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public String hget(String field) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				String value = jedis.hget(key(), field);
				return value;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				String value = jedis.hget(key(), field);
				return value;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public Long hdel(String field) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Long hdel = jedis.hdel(key(), field);
				return hdel;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
			    return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Long hdel = jedis.hdel(key(), field);
				return hdel;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public Long hlen() {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Long hlen = jedis.hlen(key());
				return hlen;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Long hlen = jedis.hlen(key());
				return hlen;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public Set<String> hkeys() {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Set<String> hkeys = jedis.hkeys(key());
				return hkeys;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Set<String> hkeys = jedis.hkeys(key());
				return hkeys;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	// Redis Set Operations
	public Long sadd(String member) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Long reply = jedis.sadd(key(), member);
				return reply;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Long reply = jedis.sadd(key(), member);
				return reply;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

//Redis Set Operations
	public Long sadd(String key, String member) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Long reply = jedis.sadd(key, member);
				return reply;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Long reply = jedis.sadd(key, member);
				return reply;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}
	
	public Long srem(String member) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Long reply = jedis.srem(key(), member);
				return reply;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Long reply = jedis.srem(key(), member);
				return reply;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}
	public Long srem(String key, String member) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Long reply = jedis.srem(key, member);
				return reply;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Long reply = jedis.srem(key, member);
				return reply;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public Set<String> smembers() {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Set<String> members = jedis.smembers(key());
				return members;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Set<String> members = jedis.smembers(key());
				return members;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}
	
	public Boolean sismember(String key, String member) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				return jedis.sismember(key, member);
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return Boolean.FALSE;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				return jedis.sismember(key, member);
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return Boolean.FALSE;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}
	
	public Long zrank(String key, String member) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				return jedis.zrank(key, member);
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				return jedis.zrank(key, member);
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}
	

	public Set<String> sinter() {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Set<String> members = jedis.sinter((String[]) keys
						.toArray(new String[0]));
				return members;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Set<String> members = jedis.sinter((String[]) keys
						.toArray(new String[0]));
				return members;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public void sinterstore(final String dstkey) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				jedis.sinterstore(dstkey,
						(String[]) keys.toArray(new String[0]));
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				jedis.sinterstore(dstkey,
						(String[]) keys.toArray(new String[0]));
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	// Redis List Operations
	public Long rpush(String string) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Long rpush = jedis.rpush(key(), string);
				return rpush;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Long rpush = jedis.rpush(key(), string);
				return rpush;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public String lset(int index, String value) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				String lset = jedis.lset(key(), index, value);
				return lset;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				String lset = jedis.lset(key(), index, value);
				return lset;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public String lindex(int index) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				String lindex = jedis.lindex(key(), index);
				return lindex;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				String lindex = jedis.lindex(key(), index);
				return lindex;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}

	}

	public Long llen() {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Long llen = jedis.llen(key());
				return llen;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Long llen = jedis.llen(key());
				return llen;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public Long lrem(int count, String value) {
		Boolean ex =false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Long lrem = jedis.lrem(key(), count, value);
				return lrem;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Long lrem = jedis.lrem(key(), count, value);
				return lrem;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public List<String> lrange(int start, int end) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				List<String> lrange = jedis.lrange(key(), start, end);
				return lrange;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				List<String> lrange = jedis.lrange(key(), start, end);
				return lrange;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	// Redis SortedSet Operations
	public Set<String> zrange(int start, int end) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Set<String> zrange = jedis.zrange(key(), start, end);
				return zrange;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Set<String> zrange = jedis.zrange(key(), start, end);
				return zrange;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public Set<String> zrangebyscore(double min, double max) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Set<String> zrange = jedis.zrangeByScore(key(), min, max);
				return zrange;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Set<String> zrange = jedis.zrangeByScore(key(), min, max);
				return zrange;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public Set<String> zrangebyscore(String min, String max) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Set<String> zrange = jedis.zrangeByScore(key(), min, max);
				return zrange;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Set<String> zrange = jedis.zrangeByScore(key(), min, max);
				return zrange;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public Long zadd(float score, String member) {
		Boolean ex =false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Long zadd = jedis.zadd(key(), score, member);
				return zadd;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Long zadd = jedis.zadd(key(), score, member);
				return zadd;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public Long zadd(double score, String member) {
		Boolean ex =false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Long zadd = jedis.zadd(key(), score, member);
				return zadd;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Long zadd = jedis.zadd(key(), score, member);
				return zadd;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public void zinterstore(final String dstkey, ZParams params) {
		Boolean ex =false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				jedis.zinterstore(dstkey, params,
						(String[]) keys.toArray(new String[0]));
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				jedis.zinterstore(dstkey, params,
						(String[]) keys.toArray(new String[0]));
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public Long zrem(String member) {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Long zrem = jedis.zrem(key(), member);
				return zrem;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Long zrem = jedis.zrem(key(), member);
				return zrem;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public Long zcard() {
		Boolean ex = false;
		if (isSharded) {
			ShardedJedis jedis = null;
			try {
				jedis = getShardedResource();
				Long zadd = jedis.zcard(key());
				return zadd;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenShardedResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnShardedResource(jedis);
				}
			}
		} else {
			Jedis jedis = null;
			try {
				jedis = getResource();
				Long zadd = jedis.zcard(key());
				return zadd;
			} catch (Exception e) {
				e.printStackTrace();
				ex = true;
				if (jedis != null) {
					returnBrokenResource(jedis);
					throw new JOhmException(e.getMessage(), JOhmExceptionMeta.GENERIC_EXCEPTION);
				}
				return null;
		    } finally {
				if (jedis != null && !ex) {
					returnResource(jedis);
				}
			}
		}
	}

	public Pipeline pipelined(Jedis jedis) {
		return jedis.pipelined();
	}

	public ShardedJedisPipeline shardedJedisPipelined(ShardedJedis jedis) {
		return jedis.pipelined();
}

	public void returnResource(final Jedis jedis) {
		jedisPool.returnResource(jedis);
	}
	
	public void returnBrokenResource(final Jedis jedis) {
		jedisPool.returnBrokenResource(jedis);
	}

	public void returnShardedResource(final ShardedJedis jedis) {
		shardedJedisPool.returnResource(jedis);
	}
	
	public void returnBrokenShardedResource(final ShardedJedis jedis) {
		shardedJedisPool.returnBrokenResource(jedis);
	}

	public Jedis getResource() {
		Jedis jedis;
		jedis = jedisPool.getResource();
		return jedis;
	}

	public ShardedJedis getShardedResource() {
		ShardedJedis jedis;
		jedis = shardedJedisPool.getResource();
		return jedis;
	}

	private void checkRedisLiveness() {
		if (isSharded) {
			if (shardedJedisPool == null) {
				throw new JOhmException(
						"JOhm will fail to do most useful tasks without Redis",
						JOhmExceptionMeta.NULL_JEDIS_POOL);
			}
		} else {
			if (jedisPool == null) {
				throw new JOhmException(
						"JOhm will fail to do most useful tasks without Redis",
						JOhmExceptionMeta.NULL_JEDIS_POOL);
			}
		}
	}
}
