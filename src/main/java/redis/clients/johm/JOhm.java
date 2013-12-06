package redis.clients.johm;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
        	throw new JOhmException(new InvalidFieldException(),
                    JOhmExceptionMeta.INVALID_INDEX);
        }

        Set<String> modelIdStrings = null;
        Nest nest = new Nest(clazz);
        nest.setJedisPool(jedisPool);
        try {
            Field field = clazz.getDeclaredField(attributeName);
            if (field == null) {
            	throw new JOhmException(new InvalidFieldException(),
                        JOhmExceptionMeta.NO_SUCH_FIELD_EXCEPTION);
            }
            field.setAccessible(true);
        	ModelMetaData metaDataOfClass = JOhm.models.get(clazz.getSimpleName());
        	boolean isIndexed = false;
        	boolean isReference = false;
        	boolean isAttribute = false;
        	if (metaDataOfClass != null) {
        		isIndexed = metaDataOfClass.indexedFields.containsKey(field.getName());
        		isReference = metaDataOfClass.referenceFields.containsKey(field.getName());
        		isAttribute = metaDataOfClass.attributeFields.containsKey(field.getName());
        	}else{
        		isIndexed =field.isAnnotationPresent(Indexed.class);
        		isReference = field.isAnnotationPresent(Reference.class);
        		isAttribute = field.isAnnotationPresent(Attribute.class);
        	}
            if (!isIndexed) {
                throw new JOhmException(new InvalidFieldException(),
                        JOhmExceptionMeta.MISSING_INDEXED_ANNOTATION);
            }
            if (isReference) {
                attributeName = JOhmUtils.getReferenceKeyName(field);
            }
            if (isAttribute || isReference) {
            	modelIdStrings = nest.cat(HASH_TAG).cat(attributeName)
            	.cat(attributeValue).smembers();
            }else{
            	modelIdStrings = nest.cat(attributeName)
            	.cat(attributeValue).smembers();
            }
        } catch (SecurityException e) {
        	throw new JOhmException(e,
                    JOhmExceptionMeta.SECURITY_EXCEPTION);
        } catch (NoSuchFieldException e) {
        	throw new JOhmException(e,
                    JOhmExceptionMeta.NO_SUCH_FIELD_EXCEPTION);
        }
        if (JOhmUtils.isNullOrEmpty(attributeValue)) {
        	throw new JOhmException(new InvalidFieldException(),
                    JOhmExceptionMeta.INVALID_VALUE);
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
    	List<Object> results = null;
		try{
			//Clazz Validation
			JOhmUtils.Validator.checkValidModelClazz(clazz);
			final String HASH_TAG = getHashTag(clazz);
			
			ModelMetaData metaDataOfClass = JOhm.models.get(clazz.getSimpleName());
			
			//Process "EQUALS" fields and keep track of range fields.
			Nest nest = new Nest(clazz);
			nest.setJedisPool(jedisPool);
			List<NVField> rangeFields = new ArrayList<NVField>();
			List<NVField> equalsFields = new ArrayList<NVField>();
			String attributeName;
			String referenceAttributeName;
			boolean isAttribute = false;
			boolean isReference = false;
			for(NVField nvField : attributes) {
				//Continue if condition is not 'Equals'
				if (!nvField.getConditionUsed().equals(Condition.EQUALS)) {
					rangeFields.add(nvField);
					continue;
				}

				equalsFields.add(nvField);

				//Validation of Field
				Field field = validationChecks(clazz, nvField);
				field.setAccessible(true);
				
				//Processing of Field
				if (metaDataOfClass != null) {
					isAttribute = metaDataOfClass.attributeFields.containsKey(field.getName());
					isReference = metaDataOfClass.referenceFields.containsKey(field.getName());
				}else{
					isAttribute = field.isAnnotationPresent(Attribute.class);
					isReference = field.isAnnotationPresent(Reference.class);
				}
				
				if (isAttribute || isReference) {//Do hash tagging only for attribute or reference
					if (isReference) {
						attributeName = JOhmUtils.getReferenceKeyName(field);
						referenceAttributeName = nvField.getReferenceAttributeName();
						nest.cat(HASH_TAG).cat(attributeName)
						.cat(referenceAttributeName)
						.cat(nvField.getReferenceAttributeValue()).next();
					}else{
						attributeName = nvField.getAttributeName();
						nest.cat(HASH_TAG).cat(attributeName)
						.cat(nvField.getAttributeValue()).next();
					}
				}else{//no hash tagging
					attributeName = nvField.getAttributeName();
					nest.cat(attributeName)
					.cat(nvField.getAttributeValue()).next();
				}
			}

			Set<String> modelIdStrings = new HashSet<String>();

			//If range condition exist, store the intersection that satisfy "EQUALTO" condition at a destination key. This is so that this set can be evaluated with range fields set.
			//If range condition don't exist, only EQUALTO condition exist and so, do intersection of equalto (unsorted) sets.
			String destinationKeyForEqualToMembers = null;
			if (!rangeFields.isEmpty()) {
				if (!equalsFields.isEmpty()){
					destinationKeyForEqualToMembers = nest.combineKeys();
					nest.sinterstore(destinationKeyForEqualToMembers);
					destinationKeyForEqualToMembers = destinationKeyForEqualToMembers.substring(nest.key().length() + 1, destinationKeyForEqualToMembers.length());
				}
			}else {
				modelIdStrings.addAll(nest.sinter());
			}

			//Process range fields now (if exist).
			attributeName = null;
			referenceAttributeName = null;
			String value = null;
			for (NVField rangeField:rangeFields) {
				//Validation of Field
				Field field = validationChecks(clazz, rangeField);
				field.setAccessible(true);

				//Processing of Field
				//If EQUALS condition exist, do intersection of equals fields(unsorted) set and range field(sorted) set and store the results at the key on which range operation will be done.
				//If EQUALS condition not exist, do range operation on this range field set.
				nest = new Nest(clazz);
				nest.setJedisPool(jedisPool);
				String keyNameForRange = null;
				if (!equalsFields.isEmpty()) {		
					if (metaDataOfClass != null) {
						isAttribute = metaDataOfClass.attributeFields.containsKey(field.getName());
						isReference = metaDataOfClass.referenceFields.containsKey(field.getName());
					}else{
						isAttribute = field.isAnnotationPresent(Attribute.class);
						isReference = field.isAnnotationPresent(Reference.class);
					}
					if (isAttribute || isReference) {//Do hash tagging only for attribute or reference
						if (isReference) {
							attributeName = JOhmUtils.getReferenceKeyName(field);
							referenceAttributeName=rangeField.getReferenceAttributeName();
							nest.cat(HASH_TAG).cat(attributeName)
							.cat(referenceAttributeName).next();
						}else{
							attributeName=rangeField.getAttributeName();
							nest.cat(HASH_TAG).cat(attributeName).next();
						}
					}else{//no hash tagging
						attributeName=rangeField.getAttributeName();
						nest.cat(attributeName).next();
					}
					nest.cat(destinationKeyForEqualToMembers).next();

					//Get the name of the key where the combined set is located and on which range operation needs to be done.
					keyNameForRange = nest.combineKeys();

					//Store the intersection at the key
					ZParams params = new ZParams();
					params.weights(1,0);
					nest.zinterstore(keyNameForRange, params);
				}else{
					if (metaDataOfClass != null) {
						isAttribute = metaDataOfClass.attributeFields.containsKey(field.getName());
						isReference = metaDataOfClass.referenceFields.containsKey(field.getName());
					}else{
						isAttribute = field.isAnnotationPresent(Attribute.class);
						isReference = field.isAnnotationPresent(Reference.class);
					}
					if (isAttribute || isReference) {//Do hash tagging only for attribute or reference
						if (isReference) {
							attributeName = JOhmUtils.getReferenceKeyName(field);
							referenceAttributeName=rangeField.getReferenceAttributeName();
							keyNameForRange = nest.cat(HASH_TAG).cat(attributeName)
							.cat(referenceAttributeName).key();
							
						}else{
							attributeName=rangeField.getAttributeName();
							keyNameForRange = nest.cat(HASH_TAG).cat(attributeName).key();
						}
					}else{//no hash tagging
						attributeName=rangeField.getAttributeName();
						keyNameForRange = nest.cat(attributeName).key();
					}
				}
					
				//Find the range of values stored at the key
				nest = new Nest(keyNameForRange);
				nest.setJedisPool(jedisPool);
				
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
		}catch(Exception e) {
			 throw new JOhmException(e,
	                    JOhmExceptionMeta.GENERIC_EXCEPTION);
		}
    	return (List<T>) results;
    }

    private static Field validationChecks(Class<?> clazz, NVField nvField) throws Exception {
    	Field field = null;
    	try {
    		if (!JOhmUtils.Validator.isIndexable(nvField.getAttributeName())) {
    			throw new JOhmException(new InvalidFieldException(),
                        JOhmExceptionMeta.INVALID_INDEX);
    		}

    		field = clazz.getDeclaredField(nvField.getAttributeName());
    		
    		if (field == null) {
    			throw new JOhmException(new InvalidFieldException(),
                        JOhmExceptionMeta.NO_SUCH_FIELD_EXCEPTION);
    		}
    		
    		field.setAccessible(true);

    		ModelMetaData metaDataOfClass = JOhm.models.get(clazz.getSimpleName());
    		boolean isIndexed = false;
    		boolean isReference = false;
    		boolean isComparable = false;
    		if (metaDataOfClass != null) {
    			isIndexed = metaDataOfClass.indexedFields.containsKey(field.getName());
    			isReference = metaDataOfClass.referenceFields.containsKey(field.getName());
    			isComparable = metaDataOfClass.comparableFields.containsKey(field.getName());
    		}else{
    			isIndexed = field.isAnnotationPresent(Indexed.class);
    			isReference = field.isAnnotationPresent(Reference.class);
    			isComparable = field.isAnnotationPresent(Comparable.class);
    		}
    		
    		if (!isIndexed) {
    			throw new JOhmException(new InvalidFieldException(),
    					JOhmExceptionMeta.MISSING_INDEXED_ANNOTATION);
    		}

    		if (isReference) {
    			if (nvField.getReferenceAttributeName() != null) {
    				
    				ModelMetaData metaDataOfReferenceClass = null;
    				boolean isIndexedFieldOfReference = false;
    				boolean isComparableFieldOfReference = false;
    				if (metaDataOfClass != null) {
    					metaDataOfReferenceClass = metaDataOfClass.referenceClasses.get(field.getName());
    				}
    				
    				if (metaDataOfReferenceClass != null) {
    					Field referenceField  = metaDataOfReferenceClass.allFields.get(nvField.getReferenceAttributeName());
    					if (referenceField == null) {
    						throw new JOhmException(new InvalidFieldException(),
    								JOhmExceptionMeta.NO_SUCH_FIELD_EXCEPTION);
    					}

    					isIndexedFieldOfReference = metaDataOfReferenceClass.indexedFields.containsKey(referenceField.getName());
    					isComparableFieldOfReference = metaDataOfReferenceClass.comparableFields.containsKey(referenceField.getName());
    				}else{
    					Field referenceField = field.getType().getDeclaredField(nvField.getReferenceAttributeName());
    					if (referenceField == null) {
    						throw new JOhmException(new InvalidFieldException(),
    								JOhmExceptionMeta.NO_SUCH_FIELD_EXCEPTION);
    					}
    					referenceField.setAccessible(true);
    					isIndexedFieldOfReference = referenceField.isAnnotationPresent(Indexed.class);
    					isComparableFieldOfReference = referenceField.isAnnotationPresent(Comparable.class);
    				}

    				if (!isIndexedFieldOfReference){
    					throw new JOhmException(new InvalidFieldException(),
    							JOhmExceptionMeta.MISSING_INDEXED_ANNOTATION);
    				}

    				if (nvField.getConditionUsed().equals(Condition.GREATERTHANEQUALTO) || nvField.getConditionUsed().equals(Condition.LESSTHANEQUALTO)
    						|| nvField.getConditionUsed().equals(Condition.GREATERTHAN) || nvField.getConditionUsed().equals(Condition.LESSTHAN)) {
    					if (!isComparableFieldOfReference) {
    						throw new JOhmException(new InvalidFieldException(),
    								JOhmExceptionMeta.MISSING_COMPARABLE_ANNOTATION);
    					}
    				}

    				if (JOhmUtils.isNullOrEmpty(nvField.getReferenceAttributeValue())) {
    					throw new JOhmException(new InvalidFieldException(),
    							JOhmExceptionMeta.INVALID_VALUE);
    				}
    			}
    		}

    		if (!isReference) {
    			if (nvField.getConditionUsed().equals(Condition.GREATERTHANEQUALTO) || nvField.getConditionUsed().equals(Condition.LESSTHANEQUALTO)
    					|| nvField.getConditionUsed().equals(Condition.GREATERTHAN) || nvField.getConditionUsed().equals(Condition.LESSTHAN)) {
    				if (!isComparable) {
    					throw new JOhmException(new InvalidFieldException(),
    							JOhmExceptionMeta.MISSING_COMPARABLE_ANNOTATION);
    				}
    			}

    			if (JOhmUtils.isNullOrEmpty(nvField.getAttributeValue())) {
    				throw new JOhmException(new InvalidFieldException(),
    						JOhmExceptionMeta.INVALID_VALUE);
    			}
    		}
    	} catch (SecurityException e) {
    		throw new JOhmException(e,
                    JOhmExceptionMeta.SECURITY_EXCEPTION);
    	} catch (NoSuchFieldException e) {
    		throw new JOhmException(e,
                    JOhmExceptionMeta.NO_SUCH_FIELD_EXCEPTION);
    	}

    	return field;
    }

    /**
     * Save given model to Redis. 
     * 
     * By default, this does not save all its child
     * annotated-models. If hierarchical persistence is desirable, use the
     * overloaded save interface.
     * 
     * By default, this does not do multi transaction. If multi transaction is desirable, use
     * the overloaded save interface.
     * 
     * This version of save uses caching.
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
    	
    	//Delete if exists
    	final Map<String, String> memberToBeRemovedFromSets = new HashMap<String, String>();
    	final Map<String, String> memberToBeRemovedFromSortedSets = new HashMap<String, String>();
        if (!isNew(model)) {
        	cleanUpForSave(model.getClass(), JOhmUtils.getId(model), memberToBeRemovedFromSets, memberToBeRemovedFromSortedSets, saveChildren);
        }
        
        //Initialize id and collections
        final Map<String, String> memberToBeAddedToSets = new HashMap<String, String>();
        final Map<String, ScoreField> memberToBeAddedToSortedSets = new HashMap<String, ScoreField>();
        final Nest nest = initIfNeeded(model);
        final String HASH_TAG = getHashTag(model.getClass());

        //Validate and Evaluate Fields
        final Map<String, String> hashedObject = new HashMap<String, String>();
        Map<RedisArray<Object>, Object[]> pendingArraysToPersist = new LinkedHashMap<RedisArray<Object>, Object[]>();
        ModelMetaData metaDataOfClass = models.get(model.getClass().getSimpleName());
        if (metaDataOfClass != null) {
        	evaluateCacheFields(model, metaDataOfClass, pendingArraysToPersist, hashedObject, memberToBeAddedToSets,memberToBeAddedToSortedSets, nest, saveChildren);
        }else{
        	evaluateFields(model, pendingArraysToPersist, hashedObject, memberToBeAddedToSets,memberToBeAddedToSortedSets, nest, saveChildren);
        }
        
        //Always add to the all set, to support getAll
        memberToBeAddedToSets.put(nest.cat(HASH_TAG + ":" + "all").key(), String.valueOf(JOhmUtils.getId(model)));
        
        //Do redis transaction
        if (doMulti) {
        	nest.multi(new TransactionBlock() {
        		public void execute() throws JedisException {
        			for (String key: memberToBeRemovedFromSets.keySet()) {
                		String memberOfSet = memberToBeRemovedFromSets.get(key);
                		srem(key, memberOfSet);
                	}
        		 	for (String key: memberToBeRemovedFromSortedSets.keySet()) {
        		 		String memberOfSet = memberToBeRemovedFromSortedSets.get(key);
                		zrem(key, memberOfSet);
        		 	}
        			del(nest.cat(JOhmUtils.getId(model)).key());
        			for (String key: memberToBeAddedToSets.keySet()) {
                		String memberOfSet = memberToBeAddedToSets.get(key);
                		sadd(key, memberOfSet);
                	}
                	for (String key: memberToBeAddedToSortedSets.keySet()) {
                		ScoreField scoreField = memberToBeAddedToSortedSets.get(key);
                		zadd(key, scoreField.getScore(), scoreField.getMember());
                	}
        			hmset(nest.cat(JOhmUtils.getId(model)).key(), hashedObject);
        		}
        	});
        }else{
        	for (String key: memberToBeRemovedFromSets.keySet()) {
        		String memberOfSet = memberToBeRemovedFromSets.get(key);
        		Nest nestForSet = new Nest(key);
        		nestForSet.setJedisPool(jedisPool);
        		nestForSet.srem(memberOfSet);
        	}
		 	for (String key: memberToBeRemovedFromSortedSets.keySet()) {
		 		String memberOfSet = memberToBeRemovedFromSortedSets.get(key);
		 		Nest nestForSet = new Nest(key);
		 		nestForSet.setJedisPool(jedisPool);
		 		nestForSet.zrem(memberOfSet);
		 	}
	      	nest.cat(JOhmUtils.getId(model)).del();
        	for (String key: memberToBeAddedToSets.keySet()) {
        		String memberOfSet = memberToBeAddedToSets.get(key);
        		Nest nestForSet = new Nest(key);
        		nestForSet.setJedisPool(jedisPool);
        		nestForSet.sadd(memberOfSet);
        	}
        	for (String key: memberToBeAddedToSortedSets.keySet()) {
        		ScoreField scoreField = memberToBeAddedToSortedSets.get(key);
        		Nest nestForSet = new Nest(key);
        		nestForSet.setJedisPool(jedisPool);
        		nestForSet.zadd(scoreField.getScore(), scoreField.getMember());
        	}
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
    
    private static void evaluateCacheFields(final Object model,ModelMetaData metaDataOfClass, Map<RedisArray<Object>, Object[]> pendingArraysToPersist, Map<String, String> hashedObject, Map<String, String> memberToBeAddedToSets, Map<String, ScoreField> memberToBeAddedToSortedSets, final Nest<?> nest, boolean saveChildren) {
    	String fieldName = null;
    	String fieldNameForCache = null;
    	List<Field> fieldsOfClass = new ArrayList<Field>(metaDataOfClass.allFields.values());
    	final String HASH_TAG = getHashTag(model.getClass());
    	try {
    		for (Field field : fieldsOfClass) {
    			fieldName = field.getName();
    			fieldNameForCache = field.getName();
    			field.setAccessible(true);
    			if (metaDataOfClass.collectionFields.containsKey(fieldNameForCache)
    					|| metaDataOfClass.idField.equals(fieldNameForCache)) {
    				continue;
    			}
    			if (metaDataOfClass.arrayFields.containsKey(fieldNameForCache)) {
    				Object[] backingArray = (Object[]) field.get(model);
    				int actualLength = backingArray == null ? 0
    						: backingArray.length;
    				JOhmUtils.Validator.checkValidArrayBounds(field,
    						actualLength);
    				Array annotation = field.getAnnotation(Array.class);
    				RedisArray<Object> redisArray = new RedisArray<Object>(
    						annotation.length(), annotation.of(), nest, field,
    						model);
    				pendingArraysToPersist.put(redisArray, backingArray);
    			}
    			if (metaDataOfClass.attributeFields.containsKey(fieldNameForCache)) {
    				fieldName = field.getName();
    				Object fieldValueObject = field.get(model);
    				if (fieldValueObject != null) {
    					hashedObject
    					.put(fieldName, fieldValueObject.toString());
    				}

    			}
    			if (metaDataOfClass.referenceFields.containsKey(fieldNameForCache)) {
    				fieldName = JOhmUtils.getReferenceKeyName(field);
    				Object child = field.get(model);
    				if (child != null) {
    					if (JOhmUtils.getId(child) == null) {
    						throw new JOhmException(new MissingIdException(),
    	                            JOhmExceptionMeta.MISSING_MODEL_ID);
    					}
    					if (saveChildren) {
    						save(child, saveChildren, false); // some more work to do
    					}
    					hashedObject.put(fieldName, String.valueOf(JOhmUtils
    							.getId(child)));
    				}
    			}
    			if (metaDataOfClass.indexedFields.containsKey(fieldNameForCache)) {
    				Object fieldValue = field.get(model);
    				if (fieldValue != null
    						&& metaDataOfClass.referenceFields.containsKey(fieldNameForCache)) {
    					fieldValue = JOhmUtils.getId(fieldValue);
    				}
    				if (!JOhmUtils.isNullOrEmpty(fieldValue)) {
    					if (metaDataOfClass.attributeFields.containsKey(fieldNameForCache) || metaDataOfClass.referenceFields.containsKey(fieldNameForCache)) {
    						memberToBeAddedToSets.put(nest.cat(HASH_TAG).cat(fieldName).cat(fieldValue).key(), String.valueOf(JOhmUtils.getId(model)));
    					}else{
    						memberToBeAddedToSets.put(nest.cat(fieldName).cat(fieldValue).key(), String.valueOf(JOhmUtils.getId(model)));
    					}

    					if (metaDataOfClass.comparableFields.containsKey(fieldNameForCache)) {
    						JOhmUtils.Validator.checkValidRangeIndexedAttribute(field);
    						if (metaDataOfClass.attributeFields.containsKey(fieldNameForCache) || metaDataOfClass.referenceFields.containsKey(fieldNameForCache)) {
    							memberToBeAddedToSortedSets.put(nest.cat(HASH_TAG).cat(fieldName).key(), new ScoreField(Double.valueOf(String.valueOf(fieldValue)), String.valueOf(JOhmUtils.getId(model))));
    						}else{
    							memberToBeAddedToSortedSets.put(nest.cat(fieldName).key(), new ScoreField(Double.valueOf(String.valueOf(fieldValue)), String.valueOf(JOhmUtils.getId(model))));
    						}
    					}

    					if (metaDataOfClass.referenceFields.containsKey(fieldNameForCache)) {
    						String childfieldName = null;
    						Object childModel = field.get(model);
    						ModelMetaData childMetaData = metaDataOfClass.referenceClasses.get(fieldNameForCache);
    						if (childMetaData == null) {
    							evaluateReferenceFieldInModel(model,metaDataOfClass, field, memberToBeAddedToSets, memberToBeAddedToSortedSets, nest);
    						}else {
    							List<Field> fieldsOfChildClass = new ArrayList<Field>(childMetaData.allFields.values());
    							for (Field childField : fieldsOfChildClass) {
    								childField.setAccessible(true);
    								childfieldName = childField.getName();
    								if (childMetaData.attributeFields.containsKey(childfieldName) && childMetaData.indexedFields.containsKey(childfieldName)) {
    									Object childFieldValue = childField.get(childModel);
    									if (!JOhmUtils.isNullOrEmpty(childFieldValue)) {
    										memberToBeAddedToSets.put(nest.cat(HASH_TAG).cat(fieldName).cat(childfieldName).cat(childFieldValue).key(), String.valueOf(JOhmUtils.getId(model)));

    										if (childMetaData.comparableFields.containsKey(childfieldName)) {
    											JOhmUtils.Validator.checkValidRangeIndexedAttribute(childField);
    											memberToBeAddedToSortedSets.put(nest.cat(HASH_TAG).cat(fieldName).cat(childfieldName).key(), new ScoreField(Double.valueOf(String.valueOf(childFieldValue)), String.valueOf(JOhmUtils.getId(model))));
    										}
    									}
    								}
    							}
    						}
    					}
    				}
    			}
    		}
    	} catch (IllegalArgumentException e) {
    		throw new JOhmException(e,
    				JOhmExceptionMeta.ILLEGAL_ARGUMENT_EXCEPTION);
    	} catch (IllegalAccessException e) {
    		throw new JOhmException(e,
    				JOhmExceptionMeta.ILLEGAL_ACCESS_EXCEPTION);
    	}
    }
    
    private static void evaluateFields(final Object model, Map<RedisArray<Object>, Object[]> pendingArraysToPersist, Map<String, String> hashedObject,  Map<String, String> memberToBeAddedToSets, Map<String, ScoreField> memberToBeAddedToSortedSets, final Nest nest, boolean saveChildren) {
    	String fieldName = null;
    	String fieldNameForCache = null;
    	ModelMetaData metaData = new ModelMetaData();
    	final String HASH_TAG = getHashTag(model.getClass());
    	try {
    		List<Field> fieldsOfClass = JOhmUtils.gatherAllFields(model.getClass());
    		for (Field field : fieldsOfClass) {
    			fieldName = field.getName();
    			fieldNameForCache = field.getName();
    			field.setAccessible(true);
    			metaData.allFields.put(fieldNameForCache, field);
    			metaData.annotatedFields.put(fieldNameForCache, field.getAnnotations());
    			if (JOhmUtils.detectJOhmCollection(field)
    					|| field.isAnnotationPresent(Id.class)) {
    				if (field.isAnnotationPresent(Id.class)) {
    					JOhmUtils.Validator.checkValidIdType(field);
    					metaData.idField = fieldNameForCache;
    				}else{
    					metaData.collectionFields.put(fieldNameForCache, field);
    					if (field.isAnnotationPresent(CollectionList.class)) {
    						metaData.collectionListFields.put(fieldNameForCache, field);
    					}else if (field.isAnnotationPresent(CollectionSet.class)) {
    						metaData.collectionSetFields.put(fieldNameForCache, field);
    					}else if (field.isAnnotationPresent(CollectionSortedSet.class)) {
    						metaData.collectionSortedSetFields.put(fieldNameForCache, field);
    					}else if (field.isAnnotationPresent(CollectionMap.class)) {
    						metaData.collectionMapFields.put(fieldNameForCache, field);
    					}
    					
    					if (field.isAnnotationPresent(Indexed.class)) {
    						metaData.indexedFields.put(fieldNameForCache, field);
    					}
    				}
    				continue;
    			}
    			boolean isArrayField = field.isAnnotationPresent(Array.class);
    			if (isArrayField) {
    				Object[] backingArray = (Object[]) field.get(model);
    				int actualLength = backingArray == null ? 0
    						: backingArray.length;
    				JOhmUtils.Validator.checkValidArrayBounds(field,
    						actualLength);
    				Array annotation = field.getAnnotation(Array.class);
    				RedisArray<Object> redisArray = new RedisArray<Object>(
    						annotation.length(), annotation.of(), nest, field,
    						model);
    				pendingArraysToPersist.put(redisArray, backingArray);
    				metaData.arrayFields.put(fieldNameForCache, field);
    			}
    			JOhmUtils.Validator.checkAttributeReferenceIndexRules(field);
    			boolean isAttributeField = field.isAnnotationPresent(Attribute.class);
    			if (isAttributeField) {
    				fieldName = field.getName();
    				Object fieldValueObject = field.get(model);
    				if (fieldValueObject != null) {
    					hashedObject
    					.put(fieldName, fieldValueObject.toString());
    				}
    				metaData.attributeFields.put(fieldNameForCache, field);
    			}
    			boolean isReferenceField = field.isAnnotationPresent(Reference.class);
    			if (isReferenceField) {
    				fieldName = JOhmUtils.getReferenceKeyName(field);
    				Object child = field.get(model);
    				if (child != null) {
    					if (JOhmUtils.getId(child) == null) {
    						throw new JOhmException(new MissingIdException(),
    	                            JOhmExceptionMeta.MISSING_MODEL_ID);
    					}
    					if (saveChildren) {
    						save(child, saveChildren, false); // some more work to do
    					}
    					hashedObject.put(fieldName, String.valueOf(JOhmUtils
    							.getId(child)));
    				}

    				metaData.referenceFields.put(fieldNameForCache, field);
    			}

    			boolean isIndexedField = field.isAnnotationPresent(Indexed.class);
    			if (isIndexedField) {
    				metaData.indexedFields.put(fieldNameForCache, field);
    			}

    			Object fieldValue = field.get(model);
    			if (fieldValue != null
    					&& isReferenceField) {
    				fieldValue = JOhmUtils.getId(fieldValue);
    			}

    			if (isAttributeField || isReferenceField) {
    				if (!JOhmUtils.isNullOrEmpty(fieldValue) && isIndexedField) {
    					memberToBeAddedToSets.put(nest.cat(HASH_TAG).cat(fieldName).cat(fieldValue).key(), String.valueOf(JOhmUtils.getId(model)));
    				}
    			}else{
    				if (!JOhmUtils.isNullOrEmpty(fieldValue) && isIndexedField) {
    					memberToBeAddedToSets.put(nest.cat(fieldName).cat(fieldValue).key(), String.valueOf(JOhmUtils.getId(model)));
    				}
    			}

    			boolean isComparable = field.isAnnotationPresent(Comparable.class);
    			if (isComparable) {
    				metaData.comparableFields.put(fieldNameForCache, field);
    				JOhmUtils.Validator.checkValidRangeIndexedAttribute(field);
    				if (isAttributeField || isReferenceField) {
    					if (!JOhmUtils.isNullOrEmpty(fieldValue)  && isIndexedField) {
    						memberToBeAddedToSortedSets.put(nest.cat(HASH_TAG).cat(fieldName).key(), new ScoreField(Double.valueOf(String.valueOf(fieldValue)), String.valueOf(JOhmUtils.getId(model))));
    					}
    				}else{
    					if (!JOhmUtils.isNullOrEmpty(fieldValue)  && isIndexedField) {
    						memberToBeAddedToSortedSets.put(nest.cat(fieldName).key(), new ScoreField(Double.valueOf(String.valueOf(fieldValue)), String.valueOf(JOhmUtils.getId(model))));
    					}
    				}
    			}

    			if (isReferenceField) {
    				evaluateReferenceFieldInModel(model,metaData, field, memberToBeAddedToSets, memberToBeAddedToSortedSets, nest);
    			}
    		}
    		models.putIfAbsent(model.getClass().getSimpleName(), metaData);
    	} catch (IllegalArgumentException e) {
    		throw new JOhmException(e,
    				JOhmExceptionMeta.ILLEGAL_ARGUMENT_EXCEPTION);
    	} catch (IllegalAccessException e) {
    		throw new JOhmException(e,
    				JOhmExceptionMeta.ILLEGAL_ACCESS_EXCEPTION);
    	}
    }
    
    private static void evaluateReferenceFieldInModel(Object model, ModelMetaData metaDataOfClass, Field referenceField, Map<String, String> memberToBeAddedToSets, Map<String, ScoreField> memberToBeAddedToSortedSets, final Nest<?> nest) {
    	try{
    		final String HASH_TAG = getHashTag(model.getClass());
    		String fieldNameOfReference = JOhmUtils.getReferenceKeyName(referenceField);
    		String fieldNameOfReferenceForCache = referenceField.getName();
    		Object referenceModel = referenceField.get(model);
    		if (referenceModel != null) {
    			ModelMetaData childMetaData = new ModelMetaData();
    			String childfieldName = null;
    			for (Field childField : JOhmUtils.gatherAllFields(referenceModel.getClass())) {
    				childfieldName = childField.getName();
    				childField.setAccessible(true);
    				childMetaData.allFields.put(childfieldName, childField);
    				childMetaData.annotatedFields.put(childfieldName, childField.getAnnotations());
    				boolean isAttributeFieldOfChild = childField.isAnnotationPresent(Attribute.class);
    				boolean isIndexedFieldOfChild = childField.isAnnotationPresent(Indexed.class);
    				boolean isComparableFieldOfChild = childField.isAnnotationPresent(Comparable.class);
    				if (isAttributeFieldOfChild) {
    					childMetaData.attributeFields.put(childfieldName, childField);
    				}
    				if (isIndexedFieldOfChild) {
    					childMetaData.indexedFields.put(childfieldName, childField);
    				}
    				if (isComparableFieldOfChild) {
    					childMetaData.comparableFields.put(childfieldName, childField);
    				}
    				if (isAttributeFieldOfChild && isIndexedFieldOfChild) {
    					Object childFieldValue = childField.get(referenceModel);
    					if (!JOhmUtils.isNullOrEmpty(childFieldValue)) {
    						memberToBeAddedToSets.put(nest.cat(HASH_TAG).cat(fieldNameOfReference).cat(childfieldName).cat(childFieldValue).key(), String.valueOf(JOhmUtils.getId(model)));
    					}
    					if (isComparableFieldOfChild) {
    						JOhmUtils.Validator.checkValidRangeIndexedAttribute(childField);
    						if (!JOhmUtils.isNullOrEmpty(childFieldValue)) {
    							memberToBeAddedToSortedSets.put(nest.cat(HASH_TAG).cat(fieldNameOfReference).cat(childfieldName).key(), new ScoreField(Double.valueOf(String.valueOf(childFieldValue)), String.valueOf(JOhmUtils.getId(model))));
    						}
    					}
    				}
    			}
    			metaDataOfClass.referenceClasses.put(fieldNameOfReferenceForCache, childMetaData);
    		}
    	} catch (IllegalArgumentException e) {
    		throw new JOhmException(e,
    				JOhmExceptionMeta.ILLEGAL_ARGUMENT_EXCEPTION);
    	} catch (IllegalAccessException e) {
    		throw new JOhmException(e,
    				JOhmExceptionMeta.ILLEGAL_ACCESS_EXCEPTION);
    	}
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
    	//Clean up if exists
    	final Map<String, String> memberToBeRemovedFromSets = new HashMap<String, String>();
    	final Map<String, String> memberToBeRemovedFromSortedSets = new HashMap<String, String>();
    	if (!isNew(model)) {
    		cleanUpForSave(model.getClass(), JOhmUtils.getId(model), memberToBeRemovedFromSets, memberToBeRemovedFromSortedSets, false);
    	}

    	//Initialize
    	final Map<String, String> memberToBeAddedToSets = new HashMap<String, String>();
    	final Map<String, ScoreField> memberToBeAddedToSortedSets = new HashMap<String, ScoreField>();
    	final Nest nest = initIfNeeded(model);
    	final String HASH_TAG = getHashTag(model.getClass());

    	//Validate and Evaluate Fields
    	final Map<String, String> hashedObject = new HashMap<String, String>();
    	Map<RedisArray<Object>, Object[]> pendingArraysToPersist = new LinkedHashMap<RedisArray<Object>, Object[]>();
    	ModelMetaData metaData = models.get(model.getClass().getSimpleName());
    	if (metaData != null) {
    		evaluateCacheFields(model, metaData, pendingArraysToPersist, hashedObject, memberToBeAddedToSets,memberToBeAddedToSortedSets, nest, false);
    	}else{
    		evaluateFields(model, pendingArraysToPersist, hashedObject, memberToBeAddedToSets,memberToBeAddedToSortedSets, nest, false);
    	}

    	//Always add to the all set, to support getAll
    	memberToBeAddedToSets.put(nest.cat(HASH_TAG + ":" + "all").key(), String.valueOf(JOhmUtils.getId(model)));
    	
    	//Do redis transaction
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
    			
    			del(nest.cat(JOhmUtils.getId(model)).key());
    			
    			for (String key: memberToBeAddedToSets.keySet()) {
    				String memberOfSet = memberToBeAddedToSets.get(key);
    				sadd(key, memberOfSet);
    			}
    			for (String key: memberToBeAddedToSortedSets.keySet()) {
    				ScoreField scoreField = memberToBeAddedToSortedSets.get(key);
    				zadd(key, scoreField.getScore(), scoreField.getMember());
    			}
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
           	ModelMetaData metaDataOfClass = JOhm.models.get(clazz.getSimpleName());
           	Collection<Field> fields = new ArrayList<Field>();
        	boolean isIndexable = false;
        	boolean isReference = false;
        	boolean isAttribute = false;
        	boolean isComparable = false;
        	boolean isArray = false;
            if (metaDataOfClass != null) {
            	fields = metaDataOfClass.allFields.values();
            }else{
            	fields = JOhmUtils.gatherAllFields(clazz);
            }
            
            if (deleteIndexes) {	
                // think about promoting deleteChildren as default behavior so
                // that this field lookup gets folded into that
                // if-deleteChildren block
                for (Field field : fields) {
                    field.setAccessible(true);
                	if (metaDataOfClass != null) {
                		isIndexable = metaDataOfClass.indexedFields.containsKey(field.getName());
                		isReference = metaDataOfClass.referenceFields.containsKey(field.getName());
                		isAttribute = metaDataOfClass.attributeFields.containsKey(field.getName());
                		isComparable = metaDataOfClass.comparableFields.containsKey(field.getName());
                	}else{
                		isIndexable = field.isAnnotationPresent(Indexed.class);
                		isReference = field.isAnnotationPresent(Reference.class);
                		isAttribute = field.isAnnotationPresent(Attribute.class);
                		isComparable = field.isAnnotationPresent(Comparable.class);
                	}
                	
                	if (isIndexable) {
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
                                && isReference) {
                            fieldValue = JOhmUtils.getId(fieldValue);
                        }
                        if (!JOhmUtils.isNullOrEmpty(fieldValue)) {
                        	if (isAttribute || isReference) {
                        		nest.cat(HASH_TAG).cat(field.getName()).cat(fieldValue).srem(
                        				String.valueOf(id));
                        	}else{
                        		nest.cat(field.getName()).cat(fieldValue).srem(
                        				String.valueOf(id));
                        	}
                        	
                        	if (isComparable) {
                        		if (isAttribute || isReference) {
                        			nest.cat(HASH_TAG).cat(field.getName()).zrem(
                        					String.valueOf(id));
                        		}else{
                        			nest.cat(field.getName()).zrem(
                        					String.valueOf(id));
                        		}
                        	}

                        	if (isReference) {
                        		try {
                        			String childfieldName = null;
                        			Object childModel = field.get(persistedModel);
                        			ModelMetaData metaDataOfReferenceClass = JOhm.models.get(childModel.getClass().getSimpleName());
                                   	Collection<Field> fieldsOfRerenceClass = new ArrayList<Field>();
                                	boolean isIndexableFieldOfReference = false;
                                	boolean isAttributeFieldOfReference = false;
                                	boolean isComparableFieldOfReference  = false;
                                    if (metaDataOfReferenceClass != null) {
                                    	fieldsOfRerenceClass = metaDataOfReferenceClass.allFields.values();
                                    }else{
                                    	fieldsOfRerenceClass = JOhmUtils.gatherAllFields(childModel.getClass());
                                    }
                        			for (Field childField : fieldsOfRerenceClass) {
                        				childfieldName = childField.getName();
                        				childField.setAccessible(true);
                        				if (metaDataOfReferenceClass != null) {
                        					isIndexableFieldOfReference = metaDataOfReferenceClass.indexedFields.containsKey(childfieldName);
                        					isAttributeFieldOfReference = metaDataOfReferenceClass.attributeFields.containsKey(childfieldName);
                        					isComparableFieldOfReference = metaDataOfReferenceClass.comparableFields.containsKey(childfieldName);
                                    	}else{
                                    		isIndexableFieldOfReference = childField.isAnnotationPresent(Indexed.class);
                                    		isAttributeFieldOfReference = childField.isAnnotationPresent(Attribute.class);
                                    		isComparableFieldOfReference = childField.isAnnotationPresent(Comparable.class);
                                    	}
                        				if (isAttributeFieldOfReference && isIndexableFieldOfReference) {
                        					Object childFieldValue = childField.get(childModel);
                        					if (childFieldValue != null && !JOhmUtils.isNullOrEmpty(childFieldValue)) {
                        						nest.cat(HASH_TAG).cat(field.getName()).cat(childfieldName).cat(childFieldValue).srem(
                        								String.valueOf(JOhmUtils.getId(persistedModel)));    
                        						
                        						if (isComparableFieldOfReference) {
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
                for (Field field : fields) {
                    field.setAccessible(true);
                	if (metaDataOfClass != null) {
                		isReference = metaDataOfClass.referenceFields.containsKey(field.getName());
                	}else{
                		isReference = field.isAnnotationPresent(Reference.class);
                	}
                	
                    if (isReference) {
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
                }
            }
            
            //Clean up array
            for (Field field : fields) {
            	if (metaDataOfClass != null) {
            		isArray = metaDataOfClass.arrayFields.containsKey(field.getName());
            	}else{
            		isArray = field.isAnnotationPresent(Array.class);
            	}
            	
            	if (isArray) {
            		field.setAccessible(true);
					Array annotation = field.getAnnotation(Array.class);
					RedisArray redisArray = new RedisArray(annotation
							.length(), annotation.of(), nest, field,
							persistedModel);
					redisArray.clear();
				}
            }

            // now delete parent
            deleted = nest.cat(id).del() == 1;
        }
        return deleted;
    }
    
    @SuppressWarnings("unchecked")
    private static Map<String, String> cleanUpForSave(Class<?> clazz, long id, Map<String, String> memberToBeRemovedFromSet, Map<String, String> memberToBeRemovedFromSortedSet, boolean cleanupChildren) {
    	JOhmUtils.Validator.checkValidModelClazz(clazz);
    	Object persistedModel = get(clazz, id);
    	final String HASH_TAG = getHashTag(clazz);
    	if (persistedModel != null) {
    		Nest nest = new Nest(persistedModel);
    		nest.setJedisPool(jedisPool);
    		try{
    			ModelMetaData metaDataOfClass = JOhm.models.get(clazz.getSimpleName());
    			boolean isIndexable = false;
    			boolean isReference = false;
    			boolean isAttribute = false;
    			boolean isComparable = false;
    			boolean isArray = false;
    			Collection<Field> fields = new ArrayList<Field>();
    			if (metaDataOfClass != null) {
    				fields = metaDataOfClass.allFields.values();
    			}else{
    				fields = JOhmUtils.gatherAllFields(clazz);
    			}
    			
    			String fieldName = null;
    			for (Field field : fields) {
    				fieldName = field.getName();
    				field.setAccessible(true);
    				if (metaDataOfClass != null) {
    					isIndexable = metaDataOfClass.indexedFields.containsKey(fieldName);
    					isReference = metaDataOfClass.referenceFields.containsKey(fieldName);
    					isAttribute = metaDataOfClass.attributeFields.containsKey(fieldName);
    					isComparable = metaDataOfClass.comparableFields.containsKey(fieldName);
    					isArray = metaDataOfClass.arrayFields.containsKey(fieldName);
    				}else{
    					isIndexable = field.isAnnotationPresent(Indexed.class);
    					isReference = field.isAnnotationPresent(Reference.class);
    					isAttribute = field.isAnnotationPresent(Attribute.class);
    					isComparable = field.isAnnotationPresent(Comparable.class);
    					isArray = field.isAnnotationPresent(Array.class);
    				}

    				if (isIndexable) {
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
    							&& isReference) {
    						fieldValue = JOhmUtils.getId(fieldValue);
    					}
    					if (!JOhmUtils.isNullOrEmpty(fieldValue)) {
    						if (isAttribute || isReference) {
    							memberToBeRemovedFromSet.put(nest.cat(HASH_TAG).cat(field.getName()).cat(fieldValue).key(), String.valueOf(id));

    							if (isComparable) {
    								memberToBeRemovedFromSortedSet.put(nest.cat(HASH_TAG).cat(field.getName()).key(), String.valueOf(id));
    							}
    						}else{
    							memberToBeRemovedFromSet.put(nest.cat(field.getName()).cat(fieldValue).key(), String.valueOf(id));

    							if (isComparable) {
    								memberToBeRemovedFromSortedSet.put(nest.cat(field.getName()).key(), String.valueOf(id));
    							}
    						}

    						if (isReference) {
    							String childfieldName = null;
    							Object childModel = field.get(persistedModel);
    							ModelMetaData metaDataOfReferenceClass = JOhm.models.get(childModel.getClass().getSimpleName());
                               	Collection<Field> fieldsOfRerenceClass = new ArrayList<Field>();
                            	boolean isIndexableFieldOfReference = false;
                            	boolean isAttributeFieldOfReference = false;
                            	boolean isComparableFieldOfReference  = false;
                                if (metaDataOfReferenceClass != null) {
                                	fieldsOfRerenceClass = metaDataOfReferenceClass.allFields.values();
                                }else{
                                	fieldsOfRerenceClass = JOhmUtils.gatherAllFields(childModel.getClass());
                                }
    							for (Field childField : fieldsOfRerenceClass) {
    								childfieldName = childField.getName();
    								childField.setAccessible(true);
    								if (metaDataOfReferenceClass != null) {
                    					isIndexableFieldOfReference = metaDataOfReferenceClass.indexedFields.containsKey(childfieldName);
                    					isAttributeFieldOfReference = metaDataOfReferenceClass.attributeFields.containsKey(childfieldName);
                    					isComparableFieldOfReference = metaDataOfReferenceClass.comparableFields.containsKey(childfieldName);
                                	}else{
                                		isIndexableFieldOfReference = childField.isAnnotationPresent(Indexed.class);
                                		isAttributeFieldOfReference = childField.isAnnotationPresent(Attribute.class);
                                		isComparableFieldOfReference = childField.isAnnotationPresent(Comparable.class);
                                	}
    								if (isAttributeFieldOfReference 
    										&& isIndexableFieldOfReference) {
    									Object childFieldValue = childField.get(childModel);
    									if (!JOhmUtils.isNullOrEmpty(childFieldValue)) {
    										memberToBeRemovedFromSet.put(nest.cat(HASH_TAG).cat(field.getName()).cat(childfieldName).cat(childFieldValue).key(), String.valueOf(JOhmUtils.getId(persistedModel)));
    										if (isComparableFieldOfReference) {
    											memberToBeRemovedFromSortedSet.put(nest.cat(HASH_TAG).cat(field.getName()).cat(childfieldName).key(), String.valueOf(JOhmUtils.getId(persistedModel)));
    										}
    									}
    								}
    							}
    						}
    					}
    				}

    				if (isArray) {
    					field.setAccessible(true);
    					Array annotation = field.getAnnotation(Array.class);
    					RedisArray redisArray = new RedisArray(annotation
    							.length(), annotation.of(), nest, field,
    							persistedModel);
    					redisArray.clear();
    				}
    			}

    			if (cleanupChildren) {
    				for (Field field : fields) {
    					if (metaDataOfClass != null) {
    						isReference = metaDataOfClass.referenceFields.containsKey(fieldName);
    					}else {
    						isReference = field.isAnnotationPresent(Reference.class);
    					}

    					if (isReference) {
    						field.setAccessible(true);
    						try {
    							Object child = field.get(persistedModel);
    							if (child != null) {
    								cleanUpForSave(child.getClass(),
    										JOhmUtils.getId(child), memberToBeRemovedFromSet,
    										memberToBeRemovedFromSortedSet,
    										cleanupChildren); // children
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
    
    static class ModelMetaData {
    	Map<String, Field> arrayFields = new HashMap<String, Field>();
    	Map<String, Field> collectionFields = new HashMap<String, Field>();
    	Map<String, Field> collectionListFields = new HashMap<String, Field>();
    	Map<String, Field> collectionSetFields = new HashMap<String, Field>();
    	Map<String, Field> collectionSortedSetFields = new HashMap<String, Field>();
    	Map<String, Field> collectionMapFields = new HashMap<String, Field>();
    	Map<String, Field> attributeFields = new HashMap<String, Field>();
    	Map<String, Field> referenceFields = new HashMap<String, Field>();
    	Map<String, Field> allFields = new HashMap<String, Field>();
    	Map<String, Field> indexedFields = new HashMap<String, Field>();
    	Map<String, Field> comparableFields = new HashMap<String, Field>();
    	Map<String, Annotation[]> annotatedFields = new HashMap<String, Annotation[]>();
    	Map<String, ModelMetaData> referenceClasses = new HashMap<String, ModelMetaData>();
    	String idField = null;
    }
    
    static final ConcurrentHashMap<String, ModelMetaData> models = new ConcurrentHashMap<String, ModelMetaData>();
}
