package redis.clients.johm;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.TransactionBlock;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.johm.NVField.Condition;
import redis.clients.johm.collections.RedisArray;

/**
 * JOhm serves as the delegate responsible for heavy-lifting all mapping
 * operations between the Object Models at one end and Redis Persistence Models
 * on the other.
 */
public final class JOhm {
    private static JedisPool jedisPool;
    private static final String INF_PLUS = "+inf";
    private static final String INF_MINUS = "-inf";

    /**
     * Read the id from the given model. This operation will typically be useful
     * only after an initial interaction with Redis in the form of a call to
     * save().
     */
    public static Long getId(final Object model) {
        return JOhmUtils.getId(model);
    }

    /**
     * Check if given model is in the new state with an uninitialized id.
     */
    public static boolean isNew(final Object model) {
        return JOhmUtils.isNew(model);
    }

    /**
     * Load the model persisted in Redis looking it up by its id and Class type.
     * 
     * @param <T>
     * @param clazz
     * @param id
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> T get(Class<?> clazz, long id) {
        JOhmUtils.Validator.checkValidModelClazz(clazz);

        Nest nest = new Nest(clazz);
        nest.setJedisPool(jedisPool);
        if (!nest.cat(id).exists()) {
            return null;
        }

        Object newInstance;
        try {
            newInstance = clazz.newInstance();
            JOhmUtils.loadId(newInstance, id);
            JOhmUtils.initCollections(newInstance, nest);

            Map<String, String> hashedObject = nest.cat(id).hgetAll();
            for (Field field : JOhmUtils.gatherAllFields(clazz)) {
                fillField(hashedObject, newInstance, field);
                fillArrayField(nest, newInstance, field);
            }

            return (T) newInstance;
        } catch (InstantiationException e) {
            throw new JOhmException(e,
                    JOhmExceptionMeta.INSTANTIATION_EXCEPTION);
        } catch (IllegalAccessException e) {
            throw new JOhmException(e,
                    JOhmExceptionMeta.ILLEGAL_ACCESS_EXCEPTION);
        }
    }

    /**
     * Search a Model in redis index using its attribute's given name/value
     * pair. This can potentially return more than 1 matches if some indexed
     * Model's have identical attributeValue for given attributeName.
     * 
     * @param clazz
     *            Class of Model annotated-type to search
     * @param attributeName
     *            Name of Model's attribute to search
     * @param attributeValue
     *            Attribute's value to search in index
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> find(Class<?> clazz, String attributeName,
            Object attributeValue) {
        JOhmUtils.Validator.checkValidModelClazz(clazz);
        final String HASH_TAG = getHashTag(clazz);
        List<Object> results = null;
        if (!JOhmUtils.Validator.isIndexable(attributeName)) {
            throw new InvalidFieldException();
        }

        Set<String> modelIdStrings = null;
        Nest nest = new Nest(clazz);
        nest.setJedisPool(jedisPool);
        try {
            Field field = clazz.getDeclaredField(attributeName);
            field.setAccessible(true);
            if (!field.isAnnotationPresent(Indexed.class)) {
                throw new InvalidFieldException();
            }
            if (field.isAnnotationPresent(Reference.class)) {
                attributeName = JOhmUtils.getReferenceKeyName(field);
            }
            if (field.isAnnotationPresent(Attribute.class) || field.isAnnotationPresent(Reference.class)) {
            	modelIdStrings = nest.cat(HASH_TAG).cat(attributeName)
            	.cat(attributeValue).smembers();
            }else{
            	modelIdStrings = nest.cat(attributeName)
            	.cat(attributeValue).smembers();
            }
        } catch (SecurityException e) {
            throw new InvalidFieldException();
        } catch (NoSuchFieldException e) {
            throw new InvalidFieldException();
        }
        if (JOhmUtils.isNullOrEmpty(attributeValue)) {
            throw new InvalidFieldException();
        }
        if (modelIdStrings != null) {
            // TODO: Do this lazy
            results = new ArrayList<Object>();
            Object indexed = null;
            for (String modelIdString : modelIdStrings) {
                indexed = get(clazz, Long.parseLong(modelIdString));
                if (indexed != null) {
                    results.add(indexed);
                }
            }
        }
        return (List<T>) results;
    }
    
    /**
     * Search a Model in redis index using its attribute's given name/value
     * pair with condition specified.
     * 
     * @param clazz
     *            Class of Model annotated-type to search
     * @param returnOnlyIds
     * @param attributes
     *            The attributes you are searching
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> find(Class<?> clazz, boolean returnOnlyIds, NVField... attributes) {
    	//Clazz Validation
    	JOhmUtils.Validator.checkValidModelClazz(clazz);
    	final String HASH_TAG = getHashTag(clazz);

    	//Process "EQUALTO" fields and keep track of range fields.
    	List<Object> results = null;
    	Nest nest = new Nest(clazz);
    	nest.setJedisPool(jedisPool);
    	List<NVField> rangeFields = new ArrayList<NVField>();
    	for(NVField nvField : attributes) {
    		//Continue if condition is not 'Equals'
    		if (!nvField.getConditionUsed().equals(Condition.EQUALS)) {
    			rangeFields.add(nvField);
    			continue;
    		}

    		//Do the validation of fields and get the attributeName and referenceAttributeName.
    		String attributeName;
    		String referenceAttributeName = null;
    		try{
    			Field field = validationChecks(clazz, nvField);
    			if (field.isAnnotationPresent(Reference.class)) {
    				attributeName = JOhmUtils.getReferenceKeyName(field);
    			}else{
    				attributeName = nvField.getAttributeName();
    			}
    			referenceAttributeName = nvField.getReferenceAttributeName();
    			
    			if (nvField.getConditionUsed().equals(Condition.EQUALS)) {
    				if (field.isAnnotationPresent(Attribute.class) || field.isAnnotationPresent(Reference.class)) {
    					if (referenceAttributeName != null){
    						nest.cat(HASH_TAG).cat(attributeName)
    						.cat(referenceAttributeName)
    						.cat(nvField.getReferenceAttributeValue()).next();
    					}else{
    						nest.cat(HASH_TAG).cat(attributeName)
    						.cat(nvField.getAttributeValue()).next();
    					}
    				}else{
    					if (referenceAttributeName != null){
    						nest.cat(attributeName)
    						.cat(referenceAttributeName)
    						.cat(nvField.getReferenceAttributeValue()).next();
    					}else{
    						nest.cat(attributeName)
    						.cat(nvField.getAttributeValue()).next();
    					}
    				}
    			}
    		}catch(Exception e) {
    			throw new InvalidFieldException();
    		}
    	}

    	Set<String> modelIdStrings = new HashSet<String>();

    	//Process range fields now.	
    	if (!rangeFields.isEmpty()) {//Range condition exist
    		//Store the intersection that satisfy "EQUALTO" condition at a destination key if there are more conditions to evaluate
    		String destinationKeyForEqualToMembers = null;
    		if (nest.keys() != null && !nest.keys().isEmpty()){
    			destinationKeyForEqualToMembers = nest.combineKeys();
    			nest.sinterstore(destinationKeyForEqualToMembers);
    			destinationKeyForEqualToMembers = destinationKeyForEqualToMembers.substring(nest.key().length() + 1, destinationKeyForEqualToMembers.length());
    		}

    		for (NVField rangeField:rangeFields) {
    			String attributeName;
    			String referenceAttributeName = null;
    			try{
    				//Do the validation and get the attributeName and referenceAttributeName.
    				Field field = validationChecks(clazz, rangeField);
    				if (field.isAnnotationPresent(Reference.class)) {
    					attributeName = JOhmUtils.getReferenceKeyName(field);
    				}else{
    					attributeName=rangeField.getAttributeName();
    				}
    				referenceAttributeName=rangeField.getReferenceAttributeName();

    				//Intersection of sorted and unsorted set.
    				if (destinationKeyForEqualToMembers != null) {//EQUALTo condition exist 
    					nest = new Nest(clazz);
    					nest.setJedisPool(jedisPool);
    					if (field.isAnnotationPresent(Attribute.class) || field.isAnnotationPresent(Reference.class)) {
    						if (referenceAttributeName != null) {
    							nest.cat(HASH_TAG).cat(attributeName)
    							.cat(referenceAttributeName).next();
    						}else{
    							nest.cat(HASH_TAG).cat(attributeName).next();
    						}
    					}else{
    						if (referenceAttributeName != null) {
    							nest.cat(attributeName)
    							.cat(referenceAttributeName).next();
    						}else{
    							nest.cat(attributeName).next();
    						}
    					}
    					nest.cat(destinationKeyForEqualToMembers).next();

    					//Get the name of the key where the combined set is located and on which range operation needs to be done.
    					String keyNameForRange = nest.combineKeys();

    					//Store the intersection at the key
    					ZParams params = new ZParams();
    					params.weights(1,0);
    					nest.zinterstore(keyNameForRange, params);

    					//Reinitialize nest in this case with new keyNameForRange
    					nest = new Nest(keyNameForRange);
    					nest.setJedisPool(jedisPool);
    				}else{//EQUALTo condition not exist 
    					nest = new Nest(clazz);
    					nest.setJedisPool(jedisPool);

    					//Get the range and add the result to modelIdStrings.
    					if (field.isAnnotationPresent(Attribute.class) || field.isAnnotationPresent(Reference.class)) {
    						if (referenceAttributeName != null){
    							nest.cat(HASH_TAG).cat(attributeName)
    							.cat(referenceAttributeName);
    						}else{
    							nest.cat(HASH_TAG).cat(attributeName);
    						}
    					}else{
    						if (referenceAttributeName != null){
    							nest.cat(attributeName)
    							.cat(referenceAttributeName);
    						}else{
    							nest.cat(attributeName);
    						}
    					}
    				}
    			}catch(Exception e) {
    				throw new InvalidFieldException();
    			}

    			//Do the range operations
    			String value = null;
    			if (referenceAttributeName != null){
    				value = String.valueOf(rangeField.getReferenceAttributeValue());
    			}else{
    				value = String.valueOf(rangeField.getAttributeValue());
    			}

    			if (rangeField.getConditionUsed().equals(Condition.GREATERTHANEQUALTO)) {
    				if (modelIdStrings.isEmpty()) {
    					modelIdStrings.addAll(nest.zrangebyscore(value, INF_PLUS));
    				}else{
    					modelIdStrings.retainAll(nest.zrangebyscore(value, INF_PLUS));
    				}
    			}else if (rangeField.getConditionUsed().equals(Condition.GREATERTHAN)) {
    				if (modelIdStrings.isEmpty()) {
    					modelIdStrings.addAll(nest.zrangebyscore("(" + value, INF_PLUS));
    				}else{
    					modelIdStrings.retainAll(nest.zrangebyscore("(" + value, INF_PLUS));
    				}
    			}else if (rangeField.getConditionUsed().equals(Condition.LESSTHANEQUALTO)) {
    				if (modelIdStrings.isEmpty()) {
    					modelIdStrings.addAll(nest.zrangebyscore(INF_MINUS, value));
    				}else{
    					modelIdStrings.retainAll(nest.zrangebyscore(INF_MINUS, value));
    				}
    			}else if (rangeField.getConditionUsed().equals(Condition.LESSTHAN)) {
    				if (modelIdStrings.isEmpty()) {
    					modelIdStrings.addAll(nest.zrangebyscore(INF_MINUS,"(" + value));
    				}else{
    					modelIdStrings.retainAll(nest.zrangebyscore(INF_MINUS,"(" + value));
    				}
    			}
    		}
    	}else{//No range condition exist, only EQUALTO condition exist
    		modelIdStrings.addAll(nest.sinter());
    	}

    	//Get the result
    	if (returnOnlyIds){
    		if (modelIdStrings != null) {
    			results = new ArrayList<Object>();
    			results.addAll(modelIdStrings);
    		}
    	}else{
    		if (modelIdStrings != null) {
    			results = new ArrayList<Object>();
    			Object indexed = null;
    			for (String modelIdString : modelIdStrings) {
    				indexed = get(clazz, Integer.parseInt(modelIdString));
    				if (indexed != null) {
    					results.add(indexed);
    				}
    			}
    		}
    	}
    	return (List<T>) results;
    }

    private static Field validationChecks(Class<?> clazz, NVField nvField) throws Exception {
    	Field field = null;
    	try {
    		if (!JOhmUtils.Validator.isIndexable(nvField.getAttributeName())) {
    			throw new InvalidFieldException();
    		}
    		
    		field = clazz.getDeclaredField(nvField.getAttributeName());
    		field.setAccessible(true);
    		if (!field.isAnnotationPresent(Indexed.class)) {
    			throw new InvalidFieldException();
    		}
    		
    		if (field.isAnnotationPresent(Reference.class)) {
    			if (nvField.getReferenceAttributeName() != null) {
    				Field referenceField = field.getType().getDeclaredField(nvField.getReferenceAttributeName());
    				referenceField.setAccessible(true);
    				if (!referenceField.isAnnotationPresent(Indexed.class)) {
    					throw new InvalidFieldException();
    				}
    				if (nvField.getConditionUsed().equals(Condition.GREATERTHANEQUALTO) || nvField.getConditionUsed().equals(Condition.LESSTHANEQUALTO)
    						|| nvField.getConditionUsed().equals(Condition.GREATERTHAN) || nvField.getConditionUsed().equals(Condition.LESSTHAN)) {
    					if (!referenceField.isAnnotationPresent(Comparable.class)) {
    						throw new InvalidFieldException();
    					}
    				}
    				if (JOhmUtils.isNullOrEmpty(nvField.getReferenceAttributeValue())) {
    					throw new InvalidFieldException();
    				}
    			}
    		}else {
    			if (nvField.getConditionUsed().equals(Condition.GREATERTHANEQUALTO) || nvField.getConditionUsed().equals(Condition.LESSTHANEQUALTO)
        				|| nvField.getConditionUsed().equals(Condition.GREATERTHAN) || nvField.getConditionUsed().equals(Condition.LESSTHAN)) {
        			if (!field.isAnnotationPresent(Comparable.class)) {
        				throw new InvalidFieldException();
        			}
        		}
    			
    			if (JOhmUtils.isNullOrEmpty(nvField.getAttributeValue())) {
    				throw new InvalidFieldException();
    			}
    		}
    	} catch (SecurityException e) {
    		throw new InvalidFieldException();
    	} catch (NoSuchFieldException e) {
    		throw new InvalidFieldException();
    	}

    	return field;
    }

    /**
     * Save given model to Redis. By default, this does not save all its child
     * annotated-models. If hierarchical persistence is desirable, use the
     * overloaded save interface.
     * 
     * @param <T>
     * @param model
     * @return
     */
    public static <T> T save(final Object model) {
        return JOhm.<T> save(model, false, false);
    }

    @SuppressWarnings("unchecked")
    public static <T> T save(final Object model, boolean saveChildren, boolean doMulti) {
        if (!isNew(model)) {
            delete(model.getClass(), JOhmUtils.getId(model));
        }
        final Nest nest = initIfNeeded(model);
        final String HASH_TAG = getHashTag(model.getClass());

        final Map<String, String> hashedObject = new HashMap<String, String>();
        Map<RedisArray<Object>, Object[]> pendingArraysToPersist = null;
        try {
            String fieldName = null;
            for (Field field : JOhmUtils.gatherAllFields(model.getClass())) {
            	fieldName = null;
                field.setAccessible(true);
                if (JOhmUtils.detectJOhmCollection(field)
                        || field.isAnnotationPresent(Id.class)) {
                    if (field.isAnnotationPresent(Id.class)) {
                        JOhmUtils.Validator.checkValidIdType(field);
                    }
                    continue;
                }
                if (field.isAnnotationPresent(Array.class)) {
                    Object[] backingArray = (Object[]) field.get(model);
                    int actualLength = backingArray == null ? 0
                            : backingArray.length;
                    JOhmUtils.Validator.checkValidArrayBounds(field,
                            actualLength);
                    Array annotation = field.getAnnotation(Array.class);
                    RedisArray<Object> redisArray = new RedisArray<Object>(
                            annotation.length(), annotation.of(), nest, field,
                            model);
                    if (pendingArraysToPersist == null) {
                        pendingArraysToPersist = new LinkedHashMap<RedisArray<Object>, Object[]>();
                    }
                    pendingArraysToPersist.put(redisArray, backingArray);
                }
                JOhmUtils.Validator.checkAttributeReferenceIndexRules(field);
                if (field.isAnnotationPresent(Attribute.class)) {
                    fieldName = field.getName();
                    Object fieldValueObject = field.get(model);
                    if (fieldValueObject != null) {
                        hashedObject
                                .put(fieldName, fieldValueObject.toString());
                    }

                }
                if (field.isAnnotationPresent(Reference.class)) {
                	fieldName = JOhmUtils.getReferenceKeyName(field);
                	Object child = field.get(model);
                	if (child != null) {
                		if (JOhmUtils.getId(child) == null) {
                			throw new MissingIdException();
                		}
                		if (saveChildren) {
                			save(child, saveChildren, false); // some more work to do
                		}
                		hashedObject.put(fieldName, String.valueOf(JOhmUtils
                				.getId(child)));
                	}
                }
                if (field.isAnnotationPresent(Indexed.class)) {
                	if (fieldName == null) {
    					fieldName = field.getName();
    				}
                	Object fieldValue = field.get(model);
                	if (fieldValue != null
                			&& field.isAnnotationPresent(Reference.class)) {
                		fieldValue = JOhmUtils.getId(fieldValue);
                	}
                	if (!JOhmUtils.isNullOrEmpty(fieldValue)) {
                		if (field.isAnnotationPresent(Attribute.class) || field.isAnnotationPresent(Reference.class)) {
                			nest.cat(HASH_TAG).cat(fieldName).cat(fieldValue).sadd(
                					String.valueOf(JOhmUtils.getId(model)));
                		}else{
                			nest.cat(fieldName).cat(fieldValue).sadd(
                					String.valueOf(JOhmUtils.getId(model)));
                		}
                		
                		if (JOhmUtils.Validator.checkValidRangeIndexedAttribute(field) && field.isAnnotationPresent(Comparable.class)) {
                			if (field.isAnnotationPresent(Attribute.class) || field.isAnnotationPresent(Reference.class)) {
                				nest.cat(HASH_TAG).cat(fieldName).zadd(Double.valueOf(String.valueOf(fieldValue)), String.valueOf(JOhmUtils.getId(model)));
                			}else{
                				nest.cat(fieldName).zadd(Double.valueOf(String.valueOf(fieldValue)), String.valueOf(JOhmUtils.getId(model)));
                			}
                		}

                		if (field.isAnnotationPresent(Reference.class)) {
                			String childfieldName = null;
                			Object childModel = field.get(model);
                			for (Field childField : JOhmUtils.gatherAllFields(childModel.getClass())) {
                				childField.setAccessible(true);
                				if (childField.isAnnotationPresent(Attribute.class) && childField.isAnnotationPresent(Indexed.class)) {
                					childfieldName = childField.getName();
                					Object childFieldValue = childField.get(childModel);
                					if (!JOhmUtils.isNullOrEmpty(childFieldValue)) {
                						nest.cat(HASH_TAG).cat(fieldName).cat(childfieldName).cat(childFieldValue).sadd(
                								String.valueOf(JOhmUtils.getId(model)));  
                						
                						if (JOhmUtils.Validator.checkValidRangeIndexedAttribute(childField) && childField.isAnnotationPresent(Comparable.class)) {
                                			nest.cat(HASH_TAG).cat(fieldName).cat(childfieldName).zadd(Double.valueOf(String.valueOf(childFieldValue)), String.valueOf(JOhmUtils.getId(model)));
                                		}
                					}
                				}
                			}
                		}
                	}
                }
            }
            
            // always add to the all set, to support getAll
            nest.cat(HASH_TAG).cat("all").sadd(String.valueOf(JOhmUtils.getId(model)));
        } catch (IllegalArgumentException e) {
            throw new JOhmException(e,
                    JOhmExceptionMeta.ILLEGAL_ARGUMENT_EXCEPTION);
        } catch (IllegalAccessException e) {
            throw new JOhmException(e,
                    JOhmExceptionMeta.ILLEGAL_ACCESS_EXCEPTION);
        }

        if (doMulti) {
        	nest.multi(new TransactionBlock() {
        		public void execute() throws JedisException {
        			del(nest.cat(JOhmUtils.getId(model)).key());
        			hmset(nest.cat(JOhmUtils.getId(model)).key(), hashedObject);
        		}
        	});
        }else{
        	nest.cat(JOhmUtils.getId(model)).del();
        	nest.cat(JOhmUtils.getId(model)).hmset(hashedObject);
        }

        if (pendingArraysToPersist != null && pendingArraysToPersist.size() > 0) {
            for (Map.Entry<RedisArray<Object>, Object[]> arrayEntry : pendingArraysToPersist
                    .entrySet()) {
                arrayEntry.getKey().write(arrayEntry.getValue());
            }
        }

        return (T) model;
    }
    
     
    /**
     * Save given model to Redis using watch. This does not save all its child
     * annotated-models. If hierarchical persistence is desirable, use the
     * overloaded save interface.
     * 
     * @param <T>
     * @param model
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> T transactedSave(final Object model) {
    	final Map<String, String> memberToBeRemovedFromSets = new HashMap<String, String>();
    	final Map<String, String> memberToBeRemovedFromSortedSets = new HashMap<String, String>();
    	final String HASH_TAG = getHashTag(model.getClass());
    	if (!isNew(model)) {
        	cleanUpForSave(model.getClass(), JOhmUtils.getId(model), memberToBeRemovedFromSets, memberToBeRemovedFromSortedSets);
    	}
    	
        final Map<String, String> memberToBeAddedToSets = new HashMap<String, String>();
        final Map<String, ScoreField> memberToBeAddedToSortedSets = new HashMap<String, ScoreField>();
    	final Nest nest = initIfNeeded(model);

    	final Map<String, String> hashedObject = new HashMap<String, String>();
    	Map<RedisArray<Object>, Object[]> pendingArraysToPersist = null;
    	try {
    		String fieldName = null;
    		for (Field field : JOhmUtils.gatherAllFields(model.getClass())) {
    			fieldName = null;
    			field.setAccessible(true);
    			if (JOhmUtils.detectJOhmCollection(field)
    					|| field.isAnnotationPresent(Id.class)) {
    				if (field.isAnnotationPresent(Id.class)) {
    					JOhmUtils.Validator.checkValidIdType(field);
    				}
    				continue;
    			}
    			if (field.isAnnotationPresent(Array.class)) {
    				Object[] backingArray = (Object[]) field.get(model);
    				int actualLength = backingArray == null ? 0
    						: backingArray.length;
    				JOhmUtils.Validator.checkValidArrayBounds(field,
    						actualLength);
    				Array annotation = field.getAnnotation(Array.class);
    				RedisArray<Object> redisArray = new RedisArray<Object>(
    						annotation.length(), annotation.of(), nest, field,
    						model);
    				if (pendingArraysToPersist == null) {
    					pendingArraysToPersist = new LinkedHashMap<RedisArray<Object>, Object[]>();
    				}
    				pendingArraysToPersist.put(redisArray, backingArray);
    			}
    			JOhmUtils.Validator.checkAttributeReferenceIndexRules(field);
    			if (field.isAnnotationPresent(Attribute.class)) {
    				fieldName = field.getName();
    				Object fieldValueObject = field.get(model);
    				if (fieldValueObject != null) {
    					hashedObject
    					.put(fieldName, fieldValueObject.toString());
    				}

    			}
    			if (field.isAnnotationPresent(Reference.class)) {
    				fieldName = JOhmUtils.getReferenceKeyName(field);
    				Object child = field.get(model);
    				if (child != null) {
    					if (JOhmUtils.getId(child) == null) {
    						throw new MissingIdException();
    					}
    					hashedObject.put(fieldName, String.valueOf(JOhmUtils
    							.getId(child)));
    				}
    			}
    			if (field.isAnnotationPresent(Indexed.class)) {
    				if (fieldName == null) {
    					fieldName = field.getName();
    				}
    				Object fieldValue = field.get(model);
    				if (fieldValue != null
    						&& field.isAnnotationPresent(Reference.class)) {
    					fieldValue = JOhmUtils.getId(fieldValue);
    				}
    				if (!JOhmUtils.isNullOrEmpty(fieldValue)) {
    					if (field.isAnnotationPresent(Attribute.class) || field.isAnnotationPresent(Reference.class)) {
    						memberToBeAddedToSets.put(nest.cat(HASH_TAG).cat(fieldName).cat(fieldValue).key(), String.valueOf(JOhmUtils.getId(model)));
    					}else{
    						memberToBeAddedToSets.put(nest.cat(fieldName).cat(fieldValue).key(), String.valueOf(JOhmUtils.getId(model)));
    					}
    					
    					if (JOhmUtils.Validator.checkValidRangeIndexedAttribute(field) && field.isAnnotationPresent(Comparable.class)) {
    						if (field.isAnnotationPresent(Attribute.class) || field.isAnnotationPresent(Reference.class)) {
    							memberToBeAddedToSortedSets.put(nest.cat(HASH_TAG).cat(fieldName).key(),  new ScoreField(Double.valueOf(String.valueOf(fieldValue)), String.valueOf(JOhmUtils.getId(model))));
    						}else{
    							memberToBeAddedToSortedSets.put(nest.cat(fieldName).key(),  new ScoreField(Double.valueOf(String.valueOf(fieldValue)), String.valueOf(JOhmUtils.getId(model))));
    						}
    					}

    					if (field.isAnnotationPresent(Reference.class)) {
    						String childfieldName = null;
    						Object childModel = field.get(model);
    						for (Field childField : JOhmUtils.gatherAllFields(childModel.getClass())) {
    							childField.setAccessible(true);
    							if (childField.isAnnotationPresent(Attribute.class) && childField.isAnnotationPresent(Indexed.class)) {
    								childfieldName = childField.getName();
    								Object childFieldValue = childField.get(childModel);
    								if (childFieldValue != null && !JOhmUtils.isNullOrEmpty(childFieldValue)) {
    									memberToBeAddedToSets.put(nest.cat(HASH_TAG).cat(fieldName).cat(childfieldName).cat(childFieldValue).key(), String.valueOf(JOhmUtils.getId(model)));
    									
    									if (JOhmUtils.Validator.checkValidRangeIndexedAttribute(childField) && childField.isAnnotationPresent(Comparable.class)) {
    										memberToBeAddedToSortedSets.put(nest.cat(HASH_TAG).cat(fieldName).cat(childfieldName).key(), new ScoreField(Double.valueOf(String.valueOf(childFieldValue)), String.valueOf(JOhmUtils.getId(model))));
    									}
    								}
    							}
    						}
    					}
    				}
    			}
    		}
    		
   			// always add to the all set, to support getAll
            memberToBeAddedToSets.put(HASH_TAG + ":" + "all", String.valueOf(JOhmUtils.getId(model)));
    	} catch (IllegalArgumentException e) {
    		 throw new JOhmException(e,
                    JOhmExceptionMeta.ILLEGAL_ARGUMENT_EXCEPTION);
    	} catch (IllegalAccessException e) {
    		 throw new JOhmException(e,
                    JOhmExceptionMeta.ILLEGAL_ACCESS_EXCEPTION);
    	}

    	List<Object> response = nest.multiWithWatch(new TransactionBlock() {
    		public void execute() throws JedisException {
    		 	for (String key: memberToBeRemovedFromSets.keySet()) {
            		String memberOfSet = memberToBeRemovedFromSets.get(key);
            		srem(key, memberOfSet);
            	}
    		 	for (String key: memberToBeRemovedFromSortedSets.keySet()) {
    		 		String memberOfSet = memberToBeRemovedFromSortedSets.get(key);
            		zrem(key, memberOfSet);
    		 	}
            	for (String key: memberToBeAddedToSets.keySet()) {
            		String memberOfSet = memberToBeAddedToSets.get(key);
            		sadd(key, memberOfSet);
            	}
            	for (String key: memberToBeAddedToSortedSets.keySet()) {
            		ScoreField scoreField = memberToBeAddedToSortedSets.get(key);
            		zadd(key, scoreField.getScore(), scoreField.getMember());
            	}
    			del(nest.cat(JOhmUtils.getId(model)).key());
    			hmset(nest.cat(JOhmUtils.getId(model)).key(), hashedObject);
    		}
    	}, nest.cat(JOhmUtils.getId(model)).key());

    	if (response != null) {
    		if (pendingArraysToPersist != null && pendingArraysToPersist.size() > 0) {
    			for (Map.Entry<RedisArray<Object>, Object[]> arrayEntry : pendingArraysToPersist
    					.entrySet()) {
    				arrayEntry.getKey().write(arrayEntry.getValue());
    			}
    		}
    	}

    	return (T) model;
    }
    
    /**
     * Delete Redis-persisted model as represented by the given model Class type
     * and id.
     * 
     * @param clazz
     * @param id
     * @return
     */
    public static boolean delete(Class<?> clazz, long id) {
        return delete(clazz, id, true, false);
    }

    @SuppressWarnings("unchecked")
    public static boolean delete(Class<?> clazz, long id,
            boolean deleteIndexes, boolean deleteChildren) {
        JOhmUtils.Validator.checkValidModelClazz(clazz);
        final String HASH_TAG = getHashTag(clazz);
        boolean deleted = false;
        Object persistedModel = get(clazz, id);
        if (persistedModel != null) {
            Nest nest = new Nest(persistedModel);
            nest.setJedisPool(jedisPool);
            if (deleteIndexes) {
                // think about promoting deleteChildren as default behavior so
                // that this field lookup gets folded into that
                // if-deleteChildren block
                for (Field field : JOhmUtils.gatherAllFields(clazz)) {
                    if (field.isAnnotationPresent(Indexed.class)) {
                        field.setAccessible(true);
                        Object fieldValue = null;
                        try {
                            fieldValue = field.get(persistedModel);
                        } catch (IllegalArgumentException e) {
                            throw new JOhmException(
                                    e,
                                    JOhmExceptionMeta.ILLEGAL_ARGUMENT_EXCEPTION);
                        } catch (IllegalAccessException e) {
                            throw new JOhmException(e,
                                    JOhmExceptionMeta.ILLEGAL_ACCESS_EXCEPTION);
                        }
                        if (fieldValue != null
                                && field.isAnnotationPresent(Reference.class)) {
                            fieldValue = JOhmUtils.getId(fieldValue);
                        }
                        if (!JOhmUtils.isNullOrEmpty(fieldValue)) {
                        	if (field.isAnnotationPresent(Attribute.class) || field.isAnnotationPresent(Reference.class)) {
                        		nest.cat(HASH_TAG).cat(field.getName()).cat(fieldValue).srem(
                        				String.valueOf(id));
                        	}else{
                        		nest.cat(field.getName()).cat(fieldValue).srem(
                        				String.valueOf(id));
                        	}
                        	
                        	if (JOhmUtils.Validator.checkValidRangeIndexedAttribute(field) && field.isAnnotationPresent(Comparable.class)) {
                        		if (field.isAnnotationPresent(Attribute.class) || field.isAnnotationPresent(Reference.class)) {
                        			nest.cat(HASH_TAG).cat(field.getName()).zrem(
                        					String.valueOf(id));
                        		}else{
                        			nest.cat(field.getName()).zrem(
                        					String.valueOf(id));
                        		}
                        	}

                        	if (field.isAnnotationPresent(Reference.class)) {
                        		try {
                        			String childfieldName = null;
                        			Object childModel = field.get(persistedModel);
                        			for (Field childField : JOhmUtils.gatherAllFields(childModel.getClass())) {
                        				childField.setAccessible(true);
                        				if (childField.isAnnotationPresent(Attribute.class) && (childField.isAnnotationPresent(Indexed.class))) {
                        					childfieldName = childField.getName();
                        					Object childFieldValue = childField.get(childModel);
                        					if (childFieldValue != null && !JOhmUtils.isNullOrEmpty(childFieldValue)) {
                        						nest.cat(HASH_TAG).cat(field.getName()).cat(childfieldName).cat(childFieldValue).srem(
                        								String.valueOf(JOhmUtils.getId(persistedModel)));    
                        						
                        						if (JOhmUtils.Validator.checkValidRangeIndexedAttribute(childField) && childField.isAnnotationPresent(Comparable.class)) {
                                            		nest.cat(HASH_TAG).cat(field.getName()).cat(childfieldName).zrem(
                                            				String.valueOf(JOhmUtils.getId(persistedModel)));
                                            	}
                        					}
                        				}
                        			}
                        		} catch (IllegalArgumentException e) {
                        			throw new JOhmException(
                        					e,
                        					JOhmExceptionMeta.ILLEGAL_ARGUMENT_EXCEPTION);
                        		} catch (IllegalAccessException e) {
                        			throw new JOhmException(e,
                        					JOhmExceptionMeta.ILLEGAL_ACCESS_EXCEPTION);
                        		}
                        	}
                        }
                    }
                }
            }
            if (deleteChildren) {
                for (Field field : JOhmUtils.gatherAllFields(clazz)) {
                    if (field.isAnnotationPresent(Reference.class)) {
                        field.setAccessible(true);
                        try {
                            Object child = field.get(persistedModel);
                            if (child != null) {
                                delete(child.getClass(),
                                        JOhmUtils.getId(child), deleteIndexes,
                                        deleteChildren); // children
                            }
                        } catch (IllegalArgumentException e) {
                            throw new JOhmException(
                                    e,
                                    JOhmExceptionMeta.ILLEGAL_ARGUMENT_EXCEPTION);
                        } catch (IllegalAccessException e) {
                            throw new JOhmException(e,
                                    JOhmExceptionMeta.ILLEGAL_ACCESS_EXCEPTION);
                        }
                    }
                    if (field.isAnnotationPresent(Array.class)) {
                        field.setAccessible(true);
                        Array annotation = field.getAnnotation(Array.class);
                        RedisArray redisArray = new RedisArray(annotation
                                .length(), annotation.of(), nest, field,
                                persistedModel);
                        redisArray.clear();
                    }
                }
            }

            // now delete parent
            deleted = nest.cat(id).del() == 1;
        }
        return deleted;
    }
    
    @SuppressWarnings("unchecked")
    private static Map<String, String> cleanUpForSave(Class<?> clazz, long id, Map<String, String> memberToBeRemovedFromSet, Map<String, String> memberToBeRemovedFromSortedSet) {
    	JOhmUtils.Validator.checkValidModelClazz(clazz);
    	Object persistedModel = get(clazz, id);
    	final String HASH_TAG = getHashTag(clazz);
    	if (persistedModel != null) {
    		Nest nest = new Nest(persistedModel);
    		nest.setJedisPool(jedisPool);
    		try{
    			for (Field field : JOhmUtils.gatherAllFields(clazz)) {
    				if (field.isAnnotationPresent(Indexed.class)) {
    					field.setAccessible(true);
    					Object fieldValue = null;
    					try {
    						fieldValue = field.get(persistedModel);
    					} catch (IllegalArgumentException e) {
    						throw new JOhmException(
    								e,
    								JOhmExceptionMeta.ILLEGAL_ARGUMENT_EXCEPTION);
    					} catch (IllegalAccessException e) {
    						throw new JOhmException(e,
    								JOhmExceptionMeta.ILLEGAL_ACCESS_EXCEPTION);
    					}
    					if (fieldValue != null
    							&& field.isAnnotationPresent(Reference.class)) {
    						fieldValue = JOhmUtils.getId(fieldValue);
    					}
    					if (!JOhmUtils.isNullOrEmpty(fieldValue)) {
    						if (field.isAnnotationPresent(Attribute.class) || field.isAnnotationPresent(Reference.class)) {
    							memberToBeRemovedFromSet.put(nest.cat(HASH_TAG).cat(field.getName()).cat(fieldValue).key(), String.valueOf(id));
    						}else{
    							memberToBeRemovedFromSet.put(nest.cat(field.getName()).cat(fieldValue).key(), String.valueOf(id));
    						}

    						if (JOhmUtils.Validator.checkValidRangeIndexedAttribute(field)) {
    							if (field.isAnnotationPresent(Attribute.class) || field.isAnnotationPresent(Reference.class)) {
    								memberToBeRemovedFromSortedSet.put(nest.cat(HASH_TAG).cat(field.getName()).key(), String.valueOf(id));
    							}else{
    								memberToBeRemovedFromSortedSet.put(nest.cat(field.getName()).key(), String.valueOf(id));
    							}
    						}

    						if (field.isAnnotationPresent(Reference.class)) {
    							String childfieldName = null;
    							Object childModel = field.get(persistedModel);
    							for (Field childField : JOhmUtils.gatherAllFields(childModel.getClass())) {
    								childField.setAccessible(true);
    								if (childField.isAnnotationPresent(Attribute.class) && (childField.isAnnotationPresent(Indexed.class))) {
    									childfieldName = childField.getName();
    									Object childFieldValue = childField.get(childModel);
    									if (!JOhmUtils.isNullOrEmpty(childFieldValue)) {
    										memberToBeRemovedFromSet.put(nest.cat(HASH_TAG).cat(field.getName()).cat(childfieldName).cat(childFieldValue).key(), String.valueOf(JOhmUtils.getId(persistedModel)));
    										if (JOhmUtils.Validator.checkValidRangeIndexedAttribute(field)) {
    											memberToBeRemovedFromSortedSet.put(nest.cat(HASH_TAG).cat(field.getName()).cat(childfieldName).key(), String.valueOf(JOhmUtils.getId(persistedModel)));
    										}
    									}
    								}
    							}
    						}
    					}
    				}
    			}
    		} catch (IllegalArgumentException e) {
    			throw new JOhmException(
    					e,
    					JOhmExceptionMeta.ILLEGAL_ARGUMENT_EXCEPTION);
    		} catch (IllegalAccessException e) {
    			throw new JOhmException(e,
    					JOhmExceptionMeta.ILLEGAL_ACCESS_EXCEPTION);
    		}
    	}
    	return memberToBeRemovedFromSet;
    }

    /**
     * Inject JedisPool into JOhm. This is a mandatory JOhm setup operation.
     * 
     * @param jedisPool
     */
    public static void setPool(final JedisPool jedisPool) {
        JOhm.jedisPool = jedisPool;
    }

    private static void fillField(final Map<String, String> hashedObject,
            final Object newInstance, final Field field)
            throws IllegalAccessException {
        JOhmUtils.Validator.checkAttributeReferenceIndexRules(field);
        if (field.isAnnotationPresent(Attribute.class)) {
            field.setAccessible(true);
            if(hashedObject.get(field.getName())!=null) {
                field.set(newInstance, JOhmUtils.Convertor.convert(field,
                    hashedObject.get(field.getName())));
            }
        }
        if (field.isAnnotationPresent(Reference.class)) {
            field.setAccessible(true);
            String serializedReferenceId = hashedObject.get(JOhmUtils
                    .getReferenceKeyName(field));
            if (serializedReferenceId != null) {
                Long referenceId = Long.valueOf(serializedReferenceId);
                field.set(newInstance, get(field.getType(), referenceId));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void fillArrayField(final Nest nest, final Object model,
            final Field field) throws IllegalArgumentException,
            IllegalAccessException {
        if (field.isAnnotationPresent(Array.class)) {
            field.setAccessible(true);
            Array annotation = field.getAnnotation(Array.class);
            RedisArray redisArray = new RedisArray(annotation.length(),
                    annotation.of(), nest, field, model);
            field.set(model, redisArray.read());
        }
    }

    @SuppressWarnings("unchecked")
    private static Nest initIfNeeded(final Object model) {
        Long id = JOhmUtils.getId(model);
        Nest nest = new Nest(model);
        nest.setJedisPool(jedisPool);
        if (id == null) {
            // lazily initialize id, nest, collections
            id = nest.cat("id").incr();
            JOhmUtils.loadId(model, id);
            JOhmUtils.initCollections(model, nest);
        }
        return nest;
    }

    @SuppressWarnings("unchecked")
    public static <T> Set<T> getAll(Class<?> clazz) {
        JOhmUtils.Validator.checkValidModelClazz(clazz);
        final String HASH_TAG = getHashTag(clazz);
        Set<Object> results = null;
        Nest nest = new Nest(clazz);
        nest.setJedisPool(jedisPool);
        Set<String> modelIdStrings = nest.cat(HASH_TAG).cat("all").smembers();
        if (modelIdStrings != null) {
            results = new HashSet<Object>();
            Object indexed = null;
            for (String modelIdString : modelIdStrings) {
                indexed = get(clazz, Long.parseLong(modelIdString));
                if (indexed != null) {
                    results.add(indexed);
                }
            }
        }
        return (Set<T>) results;
    }
    
    private static String getHashTag(Class<?> clazz) {
    	return "{" + clazz.getSimpleName() + "}";
    }
}
