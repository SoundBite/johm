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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
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
     * This finder will use hashTag as part of key (if provided). This finder also supports only one hashTag.
     * 
     * @param clazz
     *            Class of Model annotated-type to search
     * @param attributeName
     *            Name of Model's attribute to search
     * @param attributeValue
     *            Attribute's value to search in index
     * @param hashTag
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> find(Class<?> clazz, String attributeName,
            Object attributeValue, String hashTag) {
        JOhmUtils.Validator.checkValidModelClazz(clazz);
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
            	if (hashTag != null && !hashTag.isEmpty()) {
            		modelIdStrings = nest.cat(hashTag).cat(attributeName)
            				.cat(attributeValue).smembers();
            	}else{
            		modelIdStrings = nest.cat(attributeName)
            				.cat(attributeValue).smembers();
            	}
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
     * If one of the fields has HashTag annotation, it will use that field name and value for HashTaggging of the keys. 
     * This finder supports only one HashTag field.
     * 
     * If there isn't any HashTag field in attributes passed, it will not apply HashTag to the keys and if you have fields with HashTag, results will be inaccurate.
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
    		if (attributes == null || attributes.length == 0) {
    			return null;
    		}
    		
    		//Clazz Validation
    		JOhmUtils.Validator.checkValidModelClazz(clazz);
    		ModelMetaData metaDataOfClass = JOhm.models.get(clazz.getSimpleName());

    		//Find hashTag and also do validation here
    		List<NVField> rangeFields = new ArrayList<NVField>();
    		List<NVField> equalsFields = new ArrayList<NVField>();
    		Map<String, Field> fields = new HashMap<String, Field>();
    		Nest nest = new Nest(clazz);
    		nest.setJedisPool(jedisPool);
    		String hashTag = null;
    		Field field =  null;
    		boolean isAttribute = false;
    		boolean isHashTag = false;
    		String referenceAttributeName = null;
    		for (NVField nvField : attributes) {
    			//Validation of Field
    			field = validationChecks(clazz, nvField);
    			field.setAccessible(true);

    			//store all fields
    			fields.put(nvField.getAttributeName(), field);
    	
    			//Get hash tag
    			if (metaDataOfClass != null) {
    				isAttribute = metaDataOfClass.attributeFields.containsKey(field.getName());
    				isHashTag = metaDataOfClass.hashTaggedFields.containsKey(field.getName());
    			}else{
    				isAttribute = field.isAnnotationPresent(Attribute.class);
    				isHashTag = field.isAnnotationPresent(HashTag.class);
    			}

    			if (isAttribute && isHashTag) {
    				if (JOhmUtils.isNullOrEmpty(nvField.getAttributeValue())) {
    					throw new JOhmException(
    							field.getName()
								+ " is hashTag and its value is null or empty",
								JOhmExceptionMeta.NULL_OR_EMPTY_VALUE_HASH_TAG);
    				}
    				hashTag = getHashTag(nvField.getAttributeName(), String.valueOf(nvField.getAttributeValue()));
    			}
    			
    			//Continue if condition is not 'Equals'
    			//Also, add to range fields
    			if (!nvField.getConditionUsed().equals(Condition.EQUALS)) {
    				rangeFields.add(nvField);
    				continue;
    			}

    			//Add equal fields
    			equalsFields.add(nvField);
    		}

    		//Store the intersection that satisfy "EQUALTO" condition at a destination key.
    		String destinationKeyForEqualToFields = getDestinationKeyOfEqualToFields(clazz, equalsFields, fields, hashTag);
    		
    		boolean isReference = false;
    		Set<String> modelIdStrings = new HashSet<String>();
    		if (rangeFields.isEmpty()) {//we are done if no range fields, get members of destinationKeyForEqualToFields
    			nest = new Nest(clazz);
    			nest.setJedisPool(jedisPool);
    			nest.cat(destinationKeyForEqualToFields);
    			modelIdStrings.addAll(nest.smembers());
    		}else {//Get intersection of range fields and destinationKeyForEqualToFields
    			referenceAttributeName = null;
    			String value = null;
    			String keyNameForRange = null;
    			for (NVField rangeField:rangeFields) {
    				//Get destination key for range fields
    				keyNameForRange = getDestinationKeyForRangeField(clazz, rangeField, fields, destinationKeyForEqualToFields, hashTag);
    				if (keyNameForRange == null) {
    					continue;
    				}

    				if (metaDataOfClass != null) {
    	    			isReference = metaDataOfClass.referenceFields.containsKey(field.getName());
    	    		}else{
    	    			isReference = field.isAnnotationPresent(Reference.class);
    	    		}
    				
    				if (isReference) {
    					referenceAttributeName=rangeField.getReferenceAttributeName();
    				}
    				
    				//Find the range of values stored at keyNameForRange
    				nest = new Nest(keyNameForRange);
    				nest.setJedisPool(jedisPool);
    				if (referenceAttributeName != null){
    					value = String.valueOf(rangeField.getReferenceAttributeValue());
    				}else{
    					value = String.valueOf(rangeField.getAttributeValue());
    				}

    				if (JOhmUtils.isNullOrEmpty(value)) {
    					continue;
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
    		}
    		
    		//Get the result
    		if (modelIdStrings != null) {
    			if (returnOnlyIds){
    				results = new ArrayList<Object>();
    				results.addAll(modelIdStrings);
    			}else{
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

    private static String getDestinationKeyOfEqualToFields(Class<?> clazz, List<NVField> equalsFields, Map<String, Field> fields, String hashTag) throws Exception{
    	if (equalsFields == null || equalsFields.isEmpty() || fields == null || fields.isEmpty()) {
    		return null;
    	}
    	
    	//Process "EQUALS" fields
    	ModelMetaData metaDataOfClass = JOhm.models.get(clazz.getSimpleName());
    	Nest nest = new Nest(clazz);
		nest.setJedisPool(jedisPool);
    	Field field =  null;
    	boolean isAttribute = false;
		boolean isReference = false;
		String attributeName = null;
		String referenceAttributeName = null;
		for(NVField nvField : equalsFields) {
			//Get field
			field = fields.get(nvField.getAttributeName());
			field.setAccessible(true);
			
			//Processing of Field
			if (metaDataOfClass != null) {
				isAttribute = metaDataOfClass.attributeFields.containsKey(field.getName());
				isReference = metaDataOfClass.referenceFields.containsKey(field.getName());
			}else{
				isAttribute = field.isAnnotationPresent(Attribute.class);
				isReference = field.isAnnotationPresent(Reference.class);
			}
			
			//Keep track of keys via NEST
			if (isAttribute || isReference) {//Do hash tagging only for attribute or reference
				if (isReference) {
					attributeName = JOhmUtils.getReferenceKeyName(field);
					referenceAttributeName = nvField.getReferenceAttributeName();
					if (hashTag != null) {
						nest.cat(hashTag).cat(attributeName)
						.cat(referenceAttributeName)
						.cat(nvField.getReferenceAttributeValue()).next();
					}else{
						nest.cat(attributeName)
						.cat(referenceAttributeName)
						.cat(nvField.getReferenceAttributeValue()).next();
					}
				}else{
					attributeName = nvField.getAttributeName();
					if (hashTag != null) {
						nest.cat(hashTag).cat(attributeName)
						.cat(nvField.getAttributeValue()).next();
					}else{
						nest.cat(attributeName)
						.cat(nvField.getAttributeValue()).next();
					}
				}
			}else{//no hash tagging
				attributeName = nvField.getAttributeName();
				nest.cat(attributeName)
				.cat(nvField.getAttributeValue()).next();
			}
		}

		String destinationKeyForEqualToMembers = nest.combineKeys();
		nest.sinterstore(destinationKeyForEqualToMembers);
		destinationKeyForEqualToMembers = destinationKeyForEqualToMembers.substring(nest.key().length() + 1, destinationKeyForEqualToMembers.length());
		
		return destinationKeyForEqualToMembers;
    }
    
    private static String getDestinationKeyForRangeField(Class<?> clazz,  NVField rangeField, Map<String, Field> fields, String destinationKeyForEqualToFields, String hashTag) {

    	if (rangeField == null || fields == null || fields.isEmpty()) {
    		return null;
    	}

    	//Processing of range field
    	String keyNameForRange = null;
    	Field field =  null;
    	Nest nest = new Nest(clazz);
    	nest.setJedisPool(jedisPool);
    	boolean isAttribute = false;
    	boolean isReference = false;
    	String attributeName = null;
    	String referenceAttributeName = null;
    	ModelMetaData metaDataOfClass = JOhm.models.get(clazz.getSimpleName());
    	
    	//Get field
    	field = fields.get(rangeField.getAttributeName());
    	field.setAccessible(true);

    	//If EQUALS condition exist, do intersection of equals fields(unsorted) set and range field(sorted) set and store the results at the key on which range operation will be done.
    	//If EQUALS condition not exist, do range operation on this range field set.
    	if (destinationKeyForEqualToFields != null) {		
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
    				if (hashTag != null) {
    					nest.cat(hashTag).cat(attributeName)
    					.cat(referenceAttributeName).next();
    				}else{
    					nest.cat(attributeName)
    					.cat(referenceAttributeName).next();
    				}
    			}else{
    				attributeName=rangeField.getAttributeName();
    				if (hashTag != null) {
    					nest.cat(hashTag).cat(attributeName).next();
    				}else{
    					nest.cat(attributeName).next();
    				}
    			}
    		}else{//no hash tagging
    			attributeName=rangeField.getAttributeName();
    			nest.cat(attributeName).next();
    		}
    		nest.cat(destinationKeyForEqualToFields).next();

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
    				if (hashTag != null) {
    					keyNameForRange = nest.cat(hashTag).cat(attributeName)
    							.cat(referenceAttributeName).key();
    				}else{
    					keyNameForRange = nest.cat(attributeName)
    							.cat(referenceAttributeName).key();
    				}

    			}else{
    				attributeName=rangeField.getAttributeName();
    				if (hashTag != null) {
    					keyNameForRange = nest.cat(hashTag).cat(attributeName).key();
    				}else{
    					keyNameForRange = nest.cat(attributeName).key();
    				}
    			}
    		}else{//no hash tagging
    			attributeName=rangeField.getAttributeName();
    			keyNameForRange = nest.cat(attributeName).key();
    		}
    	}
    	return keyNameForRange;
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
        return JOhm.<T> save(model, false, false, true);
    }
 
    @SuppressWarnings("unchecked")
    public static <T> T save(final Object model, boolean saveChildren, boolean doMulti, boolean usePipeline) {
    	
    	//Delete if exists
    	final Map<String, String> memberToBeRemovedFromSets = new HashMap<String, String>();
    	final Map<String, String> memberToBeRemovedFromSortedSets = new HashMap<String, String>();
        if (!isNew(model)) {
        	feedCleanupData(model.getClass(), JOhmUtils.getId(model), memberToBeRemovedFromSets, memberToBeRemovedFromSortedSets, saveChildren);
        }
        
        //Initialize id and collections
        final Map<String, String> memberToBeAddedToSets = new HashMap<String, String>();
        final Map<String, ScoreField> memberToBeAddedToSortedSets = new HashMap<String, ScoreField>();
        final Nest nest = initIfNeeded(model);

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
        memberToBeAddedToSets.put(nest.cat("all").key(), String.valueOf(JOhmUtils.getId(model)));
        
        //TODO: multi with pipeline
        if (doMulti) {//Do redis transaction
        	nest.multi(new TransactionBlock() {
        		public void execute() throws JedisException {
        			String memberOfSet = null;
        			for (String key: memberToBeRemovedFromSets.keySet()) {
        				memberOfSet = memberToBeRemovedFromSets.get(key);
        				srem(key, memberOfSet);
        			}
        			for (String key: memberToBeRemovedFromSortedSets.keySet()) {
        				memberOfSet = memberToBeRemovedFromSortedSets.get(key);
        				zrem(key, memberOfSet);
        			}
        			del(nest.cat(JOhmUtils.getId(model)).key());
        			for (String key: memberToBeAddedToSets.keySet()) {
        				memberOfSet = memberToBeAddedToSets.get(key);
        				sadd(key, memberOfSet);
        			}
        			ScoreField scoreField = null;
        			for (String key: memberToBeAddedToSortedSets.keySet()) {
        				scoreField = memberToBeAddedToSortedSets.get(key);
        				zadd(key, scoreField.getScore(), scoreField.getMember());
        			}
        			hmset(nest.cat(JOhmUtils.getId(model)).key(), hashedObject);
        		}
        	});
        }else{
        	if (usePipeline) {
        		Nest nestForPipeline = new Nest();
        		nestForPipeline.setJedisPool(jedisPool);
          		Jedis jedis = null;
        		try{
        			jedis = nestForPipeline.getResource();
        			Pipeline pipeline = nestForPipeline.pipelined(jedis);

        			String memberOfSet = null;
        			String keyForId = nest.cat(JOhmUtils.getId(model)).key();
        			for (String key: memberToBeRemovedFromSets.keySet()) {
        				memberOfSet = memberToBeRemovedFromSets.get(key);
        				pipeline.srem(key, memberOfSet);
        			}
        			for (String key: memberToBeRemovedFromSortedSets.keySet()) {
        				memberOfSet = memberToBeRemovedFromSortedSets.get(key);
        				pipeline.zrem(key, memberOfSet);
        			}
        			pipeline.del(keyForId);
        			for (String key: memberToBeAddedToSets.keySet()) {
        				memberOfSet = memberToBeAddedToSets.get(key);
        				pipeline.sadd(key, memberOfSet);
        			}
        			ScoreField scoreField = null;
        			for (String key: memberToBeAddedToSortedSets.keySet()) {
        				scoreField = memberToBeAddedToSortedSets.get(key);
        				pipeline.zadd(key, scoreField.getScore(), scoreField.getMember());
        			}
        			pipeline.hmset(keyForId, hashedObject);
        			pipeline.sync();
        		}finally {
        			if (jedis != null) 
        				nestForPipeline.returnResource(jedis);
        		}
        	}else{
        		String memberOfSet = null;
        		Nest nestForSet = null;
        		for (String key: memberToBeRemovedFromSets.keySet()) {
        			memberOfSet = memberToBeRemovedFromSets.get(key);
        			nestForSet = new Nest(key);
        			nestForSet.setJedisPool(jedisPool);
        			nestForSet.srem(memberOfSet);
        		}
        		for (String key: memberToBeRemovedFromSortedSets.keySet()) {
        			memberOfSet = memberToBeRemovedFromSortedSets.get(key);
        			nestForSet = new Nest(key);
        			nestForSet.setJedisPool(jedisPool);
        			nestForSet.zrem(memberOfSet);
        		}
        		nest.cat(JOhmUtils.getId(model)).del();
        		for (String key: memberToBeAddedToSets.keySet()) {
        			memberOfSet = memberToBeAddedToSets.get(key);
        			nestForSet = new Nest(key);
        			nestForSet.setJedisPool(jedisPool);
        			nestForSet.sadd(memberOfSet);
        		}
        		ScoreField scoreField = null;
        		for (String key: memberToBeAddedToSortedSets.keySet()) {
        			scoreField = memberToBeAddedToSortedSets.get(key);
        			nestForSet = new Nest(key);
        			nestForSet.setJedisPool(jedisPool);
        			nestForSet.zadd(scoreField.getScore(), scoreField.getMember());
        		}
        		nest.cat(JOhmUtils.getId(model)).hmset(hashedObject);
        	}
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
    	Object[] backingArray  = null;
    	int actualLength = 0;
    	Array annotation = null;
    	RedisArray<Object> redisArray = null;
    	Object fieldValueObject = null;
    	Object child = null;
    	Object fieldValue = null;
    	String childfieldName = null;
    	Object childModel = null;
    	ModelMetaData childMetaData = null;
    	List<Field> fieldsOfChildClass = null;
    	Object childFieldValue = null;
    	List<String> hashTags = null;
    	try {	
    		//find hash tag
    		for (Field field : fieldsOfClass) {
    			fieldNameForCache = field.getName();
    			if (metaDataOfClass.attributeFields.containsKey(fieldNameForCache)
    					&& metaDataOfClass.hashTaggedFields.containsKey(fieldNameForCache)) {
    				fieldValueObject = field.get(model);
    				if (JOhmUtils.isNullOrEmpty(fieldValueObject)) {
    					 throw new JOhmException(
    							 field.getName()
    		                                + " is hashTag and its value is null or empty",
    		                        JOhmExceptionMeta.NULL_OR_EMPTY_VALUE_HASH_TAG);
    				}

    				if (hashTags == null) {
    					hashTags = new ArrayList<String>();
    				}
    				hashTags.add(getHashTag(fieldNameForCache, String.valueOf(fieldValueObject)));
    			}
    		}
    		
    		//Keep track of fields
    		String key = null;
    		for (Field field : fieldsOfClass) {
    			fieldName = field.getName();
    			fieldNameForCache = field.getName();
    			field.setAccessible(true);
    			if (metaDataOfClass.collectionFields.containsKey(fieldNameForCache)
    					|| metaDataOfClass.idField.equals(fieldNameForCache)) {
    				continue;
    			}
    			if (metaDataOfClass.arrayFields.containsKey(fieldNameForCache)) {
    				backingArray = (Object[]) field.get(model);
    				actualLength = backingArray == null ? 0
    						: backingArray.length;
    				JOhmUtils.Validator.checkValidArrayBounds(field,
    						actualLength);
    				annotation = field.getAnnotation(Array.class);
    				redisArray = new RedisArray<Object>(
    						annotation.length(), annotation.of(), nest, field,
    						model);
    				pendingArraysToPersist.put(redisArray, backingArray);
    			}
    			if (metaDataOfClass.attributeFields.containsKey(fieldNameForCache)) {
    				fieldName = field.getName();
    				fieldValueObject = field.get(model);
    				if (fieldValueObject != null) {
    					hashedObject
    					.put(fieldName, fieldValueObject.toString());
    				}

    			}
    			if (metaDataOfClass.referenceFields.containsKey(fieldNameForCache)) {
    				fieldName = JOhmUtils.getReferenceKeyName(field);
    				child = field.get(model);
    				if (child != null) {
    					if (JOhmUtils.getId(child) == null) {
    						throw new JOhmException(new MissingIdException(),
    	                            JOhmExceptionMeta.MISSING_MODEL_ID);
    					}
    					if (saveChildren) {
    						save(child, saveChildren, false, false); // some more work to do
    					}
    					hashedObject.put(fieldName, String.valueOf(JOhmUtils
    							.getId(child)));
    				}
    			}
    			if (metaDataOfClass.indexedFields.containsKey(fieldNameForCache)) {
    				fieldValue = field.get(model);
    				if (fieldValue != null
    						&& metaDataOfClass.referenceFields.containsKey(fieldNameForCache)) {
    					fieldValue = JOhmUtils.getId(fieldValue);
    				}
    				
    				if (!JOhmUtils.isNullOrEmpty(fieldValue)) {
    					if (metaDataOfClass.attributeFields.containsKey(fieldNameForCache) || metaDataOfClass.referenceFields.containsKey(fieldNameForCache)) {
    						if (hashTags != null) {
    							for (String hashTag:hashTags) {
    								key = nest.cat(hashTag).cat(fieldName).cat(fieldValue).key();
    								memberToBeAddedToSets.put(key, String.valueOf(JOhmUtils.getId(model)));
    							}
    						}else{
    							key = nest.cat(fieldName).cat(fieldValue).key();
    							memberToBeAddedToSets.put(key, String.valueOf(JOhmUtils.getId(model)));
    						}
    					}else{
    						key = nest.cat(fieldName).cat(fieldValue).key();
    						memberToBeAddedToSets.put(key, String.valueOf(JOhmUtils.getId(model)));
    					}
    					
    					if (metaDataOfClass.comparableFields.containsKey(fieldNameForCache)) {
    						JOhmUtils.Validator.checkValidRangeIndexedAttribute(field);
    						if (metaDataOfClass.attributeFields.containsKey(fieldNameForCache) || metaDataOfClass.referenceFields.containsKey(fieldNameForCache)) {
    							if (hashTags != null) {
    								for (String hashTag:hashTags) {
    									key = nest.cat(hashTag).cat(fieldName).key();
    									memberToBeAddedToSortedSets.put(key, new ScoreField(Double.valueOf(String.valueOf(fieldValue)), String.valueOf(JOhmUtils.getId(model))));
    								}
    							}else{
    								key = nest.cat(fieldName).key();
    								memberToBeAddedToSortedSets.put(key, new ScoreField(Double.valueOf(String.valueOf(fieldValue)), String.valueOf(JOhmUtils.getId(model))));
    							}
    						}else{
    							key = nest.cat(fieldName).key();
    							memberToBeAddedToSortedSets.put(key, new ScoreField(Double.valueOf(String.valueOf(fieldValue)), String.valueOf(JOhmUtils.getId(model))));
    						}
    					}

    					if (metaDataOfClass.referenceFields.containsKey(fieldNameForCache)) {
    						childModel = field.get(model);
    						childMetaData = metaDataOfClass.referenceClasses.get(fieldNameForCache);
    						if (childMetaData == null) {
    							evaluateReferenceFieldInModel(model,metaDataOfClass, field, memberToBeAddedToSets, memberToBeAddedToSortedSets, nest, hashTags);
    						}else {
    							fieldsOfChildClass = new ArrayList<Field>(childMetaData.allFields.values());
    							for (Field childField : fieldsOfChildClass) {
    								childField.setAccessible(true);
    								childfieldName = childField.getName();
    								if (childMetaData.attributeFields.containsKey(childfieldName) && childMetaData.indexedFields.containsKey(childfieldName)) {
    									childFieldValue = childField.get(childModel);
    									if (!JOhmUtils.isNullOrEmpty(childFieldValue)) {
    										if (hashTags != null) {
    											for (String hashTag:hashTags) {
    												key = nest.cat(hashTag).cat(fieldName).cat(childfieldName).cat(childFieldValue).key();
    												memberToBeAddedToSets.put(key, String.valueOf(JOhmUtils.getId(model)));
    											}
    										}else{
    											key = nest.cat(fieldName).cat(childfieldName).cat(childFieldValue).key();
    											memberToBeAddedToSets.put(key, String.valueOf(JOhmUtils.getId(model)));
    										}

    										if (childMetaData.comparableFields.containsKey(childfieldName)) {
    											JOhmUtils.Validator.checkValidRangeIndexedAttribute(childField);
    											if (hashTags != null) {
    												for (String hashTag:hashTags) {
    													key = nest.cat(hashTag).cat(fieldName).cat(childfieldName).key();
    													memberToBeAddedToSortedSets.put(key, new ScoreField(Double.valueOf(String.valueOf(childFieldValue)), String.valueOf(JOhmUtils.getId(model))));
    												}
    											}else{
    												key = nest.cat(fieldName).cat(childfieldName).key();
    												memberToBeAddedToSortedSets.put(key, new ScoreField(Double.valueOf(String.valueOf(childFieldValue)), String.valueOf(JOhmUtils.getId(model))));
    											}
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
    	boolean isArrayField = false;
    	boolean isAttributeField = false;
    	boolean isReferenceField = false;
    	boolean isIndexedField = false;
    	boolean isHashTagField = false;
    	Object[] backingArray = null;
    	Object fieldValue = null;
    	int actualLength = 0;
    	Array annotation = null;
    	RedisArray<Object> redisArray = null;
    	Object fieldValueObject = null;
    	Object child = null;
    	boolean isComparable = false;
    	ModelMetaData metaData = new ModelMetaData();
    	try {
    		List<Field> fieldsOfClass = JOhmUtils.gatherAllFields(model.getClass());

    		//Find hash tag as well as fill cache
    		List<String> hashTags = null;
    		for (Field field : fieldsOfClass) {
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

    			//TODO: initialize array field at the same time when attribute and reference fields are inserted
    			isArrayField = field.isAnnotationPresent(Array.class);
    			if (isArrayField) {
    				metaData.arrayFields.put(fieldNameForCache, field);
    			}

    			JOhmUtils.Validator.checkAttributeReferenceIndexRules(field);
    			JOhmUtils.Validator.checkHashTagRules(field);
    			isAttributeField = field.isAnnotationPresent(Attribute.class);
    			if (isAttributeField) {
    				metaData.attributeFields.put(fieldNameForCache, field);
    				isHashTagField = field.isAnnotationPresent(HashTag.class);
    				if (isHashTagField) {
    					fieldValueObject = field.get(model);
       					metaData.hashTaggedFields.put(fieldNameForCache, field);
    					if (JOhmUtils.isNullOrEmpty(fieldValueObject)) {
    						throw new JOhmException(
    								field.getName()
    								+ " is hashTag and its value is null or empty",
    								JOhmExceptionMeta.NULL_OR_EMPTY_VALUE_HASH_TAG);
    					}
    					if (hashTags == null) {
    						hashTags = new ArrayList<String>();
    					}
    					hashTags.add(getHashTag(fieldNameForCache, String.valueOf(fieldValueObject)));
    				}
    			}

    			isReferenceField = field.isAnnotationPresent(Reference.class);
    			if (isReferenceField) {
    				metaData.referenceFields.put(fieldNameForCache, field);
    			}

    			isIndexedField = field.isAnnotationPresent(Indexed.class);
    			if (isIndexedField) {
    				metaData.indexedFields.put(fieldNameForCache, field);
    			}
    		
    			isComparable = field.isAnnotationPresent(Comparable.class);
    			if (isComparable) {
    				metaData.comparableFields.put(fieldNameForCache, field);
    			}
    		}

    		models.putIfAbsent(model.getClass().getSimpleName(), metaData);

    		//Process keys associated with fields
    		String key = null;
    		for (Field field : fieldsOfClass) {
    			fieldName = field.getName();
    			fieldNameForCache = field.getName();
    			field.setAccessible(true);
    			if (JOhmUtils.detectJOhmCollection(field)
    					|| (metaData.idField != null && metaData.idField.equalsIgnoreCase(fieldNameForCache))) {
    				continue;
    			}

    			//TODO: initialize array field at the same time when attribute and reference fields are inserted
    			isArrayField = metaData.arrayFields.containsKey(fieldNameForCache);
    			if (isArrayField) {
    				backingArray = (Object[]) field.get(model);
    				actualLength = backingArray == null ? 0
    						: backingArray.length;
    				JOhmUtils.Validator.checkValidArrayBounds(field,
    						actualLength);
    				annotation = field.getAnnotation(Array.class);
    				redisArray = new RedisArray<Object>(
    						annotation.length(), annotation.of(), nest, field,
    						model);
    				pendingArraysToPersist.put(redisArray, backingArray);
    			}

    			isAttributeField = metaData.attributeFields.containsKey(fieldNameForCache);
    			if (isAttributeField) {
    				fieldName = field.getName();
    				fieldValueObject = field.get(model);
    				if (fieldValueObject != null) {
    					hashedObject
    					.put(fieldName, String.valueOf(fieldValueObject));
    				}
    			}

    			isReferenceField = metaData.referenceFields.containsKey(fieldNameForCache);
    			if (isReferenceField) {
    				fieldName = JOhmUtils.getReferenceKeyName(field);
    				child = field.get(model);
    				if (child != null) {
    					if (JOhmUtils.getId(child) == null) {
    						throw new JOhmException(new MissingIdException(),
    								JOhmExceptionMeta.MISSING_MODEL_ID);
    					}
    					if (saveChildren) {
    						save(child, saveChildren, false, false); // some more work to do
    					}
    					hashedObject.put(fieldName, String.valueOf(JOhmUtils
    							.getId(child)));
    				}
    			}
    			
    			isIndexedField = metaData.indexedFields.containsKey(fieldNameForCache);

    			fieldValue = field.get(model);
    			if (fieldValue != null
    					&& isReferenceField) {
    				fieldValue = JOhmUtils.getId(fieldValue);
    			}

    			if (isAttributeField || isReferenceField) {
    				if (!JOhmUtils.isNullOrEmpty(fieldValue) && isIndexedField) {
    					if (hashTags != null) {
    						for (String hashTag:hashTags) {
    							key = nest.cat(hashTag).cat(fieldName).cat(fieldValue).key();
    							memberToBeAddedToSets.put(key, String.valueOf(JOhmUtils.getId(model)));
    						}
    					}else{
    						key = nest.cat(fieldName).cat(fieldValue).key();
    						memberToBeAddedToSets.put(key, String.valueOf(JOhmUtils.getId(model)));
    					}
    				}
    			}else{
    				if (!JOhmUtils.isNullOrEmpty(fieldValue) && isIndexedField) {
    					key = nest.cat(fieldName).cat(fieldValue).key();
    					memberToBeAddedToSets.put(key, String.valueOf(JOhmUtils.getId(model)));
    				}
    			}

    			isComparable = metaData.comparableFields.containsKey(fieldNameForCache);
    			if (isComparable) {
    				JOhmUtils.Validator.checkValidRangeIndexedAttribute(field);
    				if (isAttributeField || isReferenceField) {
    					if (!JOhmUtils.isNullOrEmpty(fieldValue) && isIndexedField) {
    						if (hashTags != null) {
    							for (String hashTag:hashTags) {
    								key = nest.cat(hashTag).cat(fieldName).key();
    								memberToBeAddedToSortedSets.put(key, new ScoreField(Double.valueOf(String.valueOf(fieldValue)), String.valueOf(JOhmUtils.getId(model))));
    							}
    						}else{
    							key = nest.cat(fieldName).key();
    							memberToBeAddedToSortedSets.put(key, new ScoreField(Double.valueOf(String.valueOf(fieldValue)), String.valueOf(JOhmUtils.getId(model))));
    						}
    					}
    				}else{
    					if (!JOhmUtils.isNullOrEmpty(fieldValue) && isIndexedField) {
    						key = nest.cat(fieldName).key();
    						memberToBeAddedToSortedSets.put(key, new ScoreField(Double.valueOf(String.valueOf(fieldValue)), String.valueOf(JOhmUtils.getId(model))));
    					}
    				}
    			}

    			if (isReferenceField) {
    				evaluateReferenceFieldInModel(model,metaData, field, memberToBeAddedToSets, memberToBeAddedToSortedSets, nest, hashTags);
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

    private static void evaluateReferenceFieldInModel(Object model, ModelMetaData metaDataOfClass, Field referenceField, Map<String, String> memberToBeAddedToSets, Map<String, ScoreField> memberToBeAddedToSortedSets, final Nest<?> nest, List<String> hashTags) {
    	try{
    		String fieldNameOfReference = JOhmUtils.getReferenceKeyName(referenceField);
    		String fieldNameOfReferenceForCache = referenceField.getName();
    		Object referenceModel = referenceField.get(model);
    		if (referenceModel != null) {
    			ModelMetaData childMetaData = new ModelMetaData();
    			String childfieldName = null;
    			boolean isAttributeFieldOfChild = false;
    			boolean isIndexedFieldOfChild = false;
    			boolean isComparableFieldOfChild = false;
    			Object childFieldValue = null;
    			String key = null;
    			
    			//Process keys associated with fields
    			for (Field childField : JOhmUtils.gatherAllFields(referenceModel.getClass())) {
    				childfieldName = childField.getName();
    				childField.setAccessible(true);
    				childMetaData.allFields.put(childfieldName, childField);
    				childMetaData.annotatedFields.put(childfieldName, childField.getAnnotations());
    				isAttributeFieldOfChild = childField.isAnnotationPresent(Attribute.class);
    				isIndexedFieldOfChild = childField.isAnnotationPresent(Indexed.class);
    				isComparableFieldOfChild = childField.isAnnotationPresent(Comparable.class);

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
    					childFieldValue = childField.get(referenceModel);
    					if (!JOhmUtils.isNullOrEmpty(childFieldValue)) {
    						if (hashTags != null) {
    							for (String hashTag:hashTags) {
    								key = nest.cat(hashTag).cat(fieldNameOfReference).cat(childfieldName).cat(childFieldValue).key();
    								memberToBeAddedToSets.put(key, String.valueOf(JOhmUtils.getId(model)));
    							}
    						}else{
    							key = nest.cat(fieldNameOfReference).cat(childfieldName).cat(childFieldValue).key();
    							memberToBeAddedToSets.put(key, String.valueOf(JOhmUtils.getId(model)));
    						}
    					}
    					
    					if (isComparableFieldOfChild) {
    						JOhmUtils.Validator.checkValidRangeIndexedAttribute(childField);
    						if (!JOhmUtils.isNullOrEmpty(childFieldValue)) {
    							if (hashTags != null) {
    								for (String hashTag:hashTags) {
    									key = nest.cat(hashTag).cat(fieldNameOfReference).cat(childfieldName).key();
    									memberToBeAddedToSortedSets.put(key, new ScoreField(Double.valueOf(String.valueOf(childFieldValue)), String.valueOf(JOhmUtils.getId(model))));
    								}
    							}else{
    								key = nest.cat(fieldNameOfReference).cat(childfieldName).key();
    								memberToBeAddedToSortedSets.put(key, new ScoreField(Double.valueOf(String.valueOf(childFieldValue)), String.valueOf(JOhmUtils.getId(model))));
    							}
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

    public static String getHashTag(String fieldName, String fieldValue) {
    	return "{" + fieldName + "_" +  fieldValue + "}";
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
    		feedCleanupData(model.getClass(), JOhmUtils.getId(model), memberToBeRemovedFromSets, memberToBeRemovedFromSortedSets, false);
    	}

    	//Initialize
    	final Map<String, String> memberToBeAddedToSets = new HashMap<String, String>();
    	final Map<String, ScoreField> memberToBeAddedToSortedSets = new HashMap<String, ScoreField>();
    	final Nest nest = initIfNeeded(model);

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
    	memberToBeAddedToSets.put(nest.cat("all").key(), String.valueOf(JOhmUtils.getId(model)));
    	
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

    	final Map<String, String> memberToBeRemovedFromSets = new HashMap<String, String>();
    	final Map<String, String> memberToBeRemovedFromSortedSets = new HashMap<String, String>();
    	boolean deleted = false;

    	try{
    		feedCleanupData(clazz, id, memberToBeRemovedFromSets, memberToBeRemovedFromSortedSets, false);

    		ModelMetaData metaDataOfClass = JOhm.models.get(clazz.getSimpleName());
           	Collection<Field> fields = new ArrayList<Field>();
        	boolean isReference = false;
            if (metaDataOfClass != null) {
            	fields = metaDataOfClass.allFields.values();
            }else{
            	fields = JOhmUtils.gatherAllFields(clazz);
            }
            
            if (deleteIndexes) {
            	String memberOfSet = null;
            	Nest nestForSet = null;
            	for (String key: memberToBeRemovedFromSets.keySet()) {
            		memberOfSet = memberToBeRemovedFromSets.get(key);
            		nestForSet = new Nest(key);
            		nestForSet.setJedisPool(jedisPool);
            		nestForSet.srem(memberOfSet);
            	}
            	for (String key: memberToBeRemovedFromSortedSets.keySet()) {
            		memberOfSet = memberToBeRemovedFromSortedSets.get(key);
            		nestForSet = new Nest(key);
            		nestForSet.setJedisPool(jedisPool);
            		nestForSet.zrem(memberOfSet);
            	}
            }

            Object persistedModel = get(clazz, id);

            if (persistedModel != null) {
            	if (deleteChildren) {
            		Object child = null;
            		for (Field field : fields) {
            			field.setAccessible(true);
            			if (metaDataOfClass != null) {
            				isReference = metaDataOfClass.referenceFields.containsKey(field.getName());
            			}else{
            				isReference = field.isAnnotationPresent(Reference.class);
            			}

            			if (isReference) {
            				child = field.get(persistedModel);
            				if (child != null) {
            					delete(child.getClass(),
            							JOhmUtils.getId(child), deleteIndexes,
            							deleteChildren); // children
            				}
            			}
            		}
            	}

            	Nest nest = new Nest(persistedModel);
            	nest.setJedisPool(jedisPool);
            	deleted = nest.cat(id).del() == 1;
            }
    	} catch (IllegalArgumentException e) {
    		throw new JOhmException(
    				e,
    				JOhmExceptionMeta.ILLEGAL_ARGUMENT_EXCEPTION);
    	} catch (IllegalAccessException e) {
    		throw new JOhmException(e,
    				JOhmExceptionMeta.ILLEGAL_ACCESS_EXCEPTION);
    	}
    	return deleted;
    }

    @SuppressWarnings("unchecked")
    private static void feedCleanupData(Class<?> clazz, long id, Map<String, String> memberToBeRemovedFromSet, Map<String, String> memberToBeRemovedFromSortedSet, boolean cleanupChildren) {
    	JOhmUtils.Validator.checkValidModelClazz(clazz);
    	Object persistedModel = get(clazz, id);
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
    			boolean isHashTag = false;
    			
    			//Get the fields
    			Collection<Field> fields = new ArrayList<Field>();
    			if (metaDataOfClass != null) {
    				fields = metaDataOfClass.allFields.values();
    			}else{
    				fields = JOhmUtils.gatherAllFields(clazz);
    			}

    			//Find hash tag
    			String fieldName = null;
    			List<String> hashTags = null;
    			Object fieldValue = null;
    			for (Field field : fields) {
    				fieldName = field.getName();
    				field.setAccessible(true);
    				if (metaDataOfClass != null) {
    					isAttribute = metaDataOfClass.attributeFields.containsKey(fieldName);
    					isHashTag  = metaDataOfClass.hashTaggedFields.containsKey(fieldName);
    				}else{
    					isAttribute = field.isAnnotationPresent(Attribute.class);
    					isHashTag = field.isAnnotationPresent(HashTag.class);
    				}

    				if (isAttribute && isHashTag) {
    					fieldValue = field.get(persistedModel);
    					if (JOhmUtils.isNullOrEmpty(fieldValue)) {
    						throw new JOhmException(
    								field.getName()
    								+ " is hashTag and its value is null or empty",
    								JOhmExceptionMeta.NULL_OR_EMPTY_VALUE_HASH_TAG);
    					}
    					if (hashTags == null) {
    						hashTags = new ArrayList<String>();
    					}
    					hashTags.add(getHashTag(fieldName, String.valueOf(fieldValue)));
    				}
    			}

    			//Keep track of fields to be cleaned.
    			String key = null;
    			String childfieldName = null;
    			Object childModel = null;
    			ModelMetaData metaDataOfReferenceClass = null;
    			Collection<Field> fieldsOfRerenceClass = null;
    			boolean isIndexableFieldOfReference = false;
				boolean isAttributeFieldOfReference = false;
				boolean isComparableFieldOfReference  = false;
				Object childFieldValue = null;
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
    					fieldValue = field.get(persistedModel);
    					
    					//if reference field, field value is id
    					if (fieldValue != null
    							&& isReference) {
    						fieldValue = JOhmUtils.getId(fieldValue);
    					}
    					
    					if (!JOhmUtils.isNullOrEmpty(fieldValue)) {
    						
    						//Only attributes and references are HashTagged
    						if (isAttribute || isReference) {
    							if (hashTags != null && !hashTags.isEmpty()) {
    								for (String hashTag:hashTags) {
    									key = nest.cat(hashTag).cat(field.getName()).cat(fieldValue).key();
    									memberToBeRemovedFromSet.put(key, String.valueOf(id));
    								}
    							}else{
    								key = nest.cat(field.getName()).cat(fieldValue).key();
    								memberToBeRemovedFromSet.put(key, String.valueOf(id));
    							}

    							if (isComparable) {
    								if (hashTags != null && !hashTags.isEmpty()) {
    									for (String hashTag:hashTags) {
    										key = nest.cat(hashTag).cat(field.getName()).key();
    										memberToBeRemovedFromSortedSet.put(key, String.valueOf(id));
    									}
    								}else{
    									key = nest.cat(field.getName()).key();
    									memberToBeRemovedFromSortedSet.put(key, String.valueOf(id));
    								}
    							}
    						}else{
    							memberToBeRemovedFromSet.put(nest.cat(field.getName()).cat(fieldValue).key(), String.valueOf(id));

    							if (isComparable) {
    								memberToBeRemovedFromSortedSet.put(nest.cat(field.getName()).key(), String.valueOf(id));
    							}
    						}

    						//Reference field
    						if (isReference) {
    							childModel = field.get(persistedModel);
    							metaDataOfReferenceClass = JOhm.models.get(childModel.getClass().getSimpleName());
    							fieldsOfRerenceClass = new ArrayList<Field>();
    							if (metaDataOfReferenceClass != null) {
    								fieldsOfRerenceClass = metaDataOfReferenceClass.allFields.values();
    							}else{
    								fieldsOfRerenceClass = JOhmUtils.gatherAllFields(childModel.getClass());
    							}

    							//Keep track of reference fields to be cleaned
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
    									childFieldValue = childField.get(childModel);
    									if (!JOhmUtils.isNullOrEmpty(childFieldValue)) {
    										if (hashTags != null && !hashTags.isEmpty()) {
    											for (String hashTag:hashTags) {
    												key = nest.cat(hashTag).cat(field.getName()).cat(childfieldName).cat(childFieldValue).key();
    												memberToBeRemovedFromSet.put(key, String.valueOf(JOhmUtils.getId(persistedModel)));
    											}
    										}else{
    											key = nest.cat(field.getName()).cat(childfieldName).cat(childFieldValue).key();
    											memberToBeRemovedFromSet.put(key, String.valueOf(JOhmUtils.getId(persistedModel)));
    										}

    										if (isComparableFieldOfReference) {
    											if (hashTags != null && !hashTags.isEmpty()) {
    												for (String hashTag:hashTags) {
    													key = nest.cat(hashTag).cat(field.getName()).cat(childfieldName).key();
    													memberToBeRemovedFromSortedSet.put(key, String.valueOf(JOhmUtils.getId(persistedModel)));
    												}
    											}else{
    												key = nest.cat(field.getName()).cat(childfieldName).key();
    												memberToBeRemovedFromSortedSet.put(key, String.valueOf(JOhmUtils.getId(persistedModel)));
    											}
    										}
    									}
    								}
    							}
    						}
    					}
    				}

    				//Clean array field
    				//TODO: clean this at same time when other fields are cleaned up
    				if (isArray) {
    					field.setAccessible(true);
    					Array annotation = field.getAnnotation(Array.class);
    					RedisArray redisArray = new RedisArray(annotation
    							.length(), annotation.of(), nest, field,
    							persistedModel);
    					redisArray.clear();
    				}
    			}

    			//Keep track of children to be cleaned if flag set
    			if (cleanupChildren) {
    				for (Field field : fields) {
    					if (metaDataOfClass != null) {
    						isReference = metaDataOfClass.referenceFields.containsKey(fieldName);
    					}else {
    						isReference = field.isAnnotationPresent(Reference.class);
    					}

    					if (isReference) {
    						field.setAccessible(true);
    						Object child = field.get(persistedModel);
    						if (child != null) {
    							feedCleanupData(child.getClass(),
    									JOhmUtils.getId(child), memberToBeRemovedFromSet,
    									memberToBeRemovedFromSortedSet,
    									cleanupChildren); // children
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
        Set<Object> results = null;
        Nest nest = new Nest(clazz);
        nest.setJedisPool(jedisPool);
        Set<String> modelIdStrings = nest.cat("all").smembers();
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
    	Map<String, Field> hashTaggedFields = new HashMap<String, Field>();
    	Map<String, Field> comparableFields = new HashMap<String, Field>();
    	Map<String, Annotation[]> annotatedFields = new HashMap<String, Annotation[]>();
    	Map<String, ModelMetaData> referenceClasses = new HashMap<String, ModelMetaData>();
    	String idField = null;
    }
    
    static final ConcurrentHashMap<String, ModelMetaData> models = new ConcurrentHashMap<String, ModelMetaData>();
}
