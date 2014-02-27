package redis.clients.johm;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.johm.JOhm.ModelMetaData;
import redis.clients.johm.collections.RedisList;
import redis.clients.johm.collections.RedisMap;
import redis.clients.johm.collections.RedisSet;
import redis.clients.johm.collections.RedisSortedSet;

public final class JOhmUtils {
    static String getReferenceKeyName(final Field field) {
        return field.getName() + "_id";
    }

    public static Long getId(final Object model) {
        return getId(model, true);
    }

    public static Long getId(final Object model, boolean checkValidity) {
        Long id = null;
        if (model != null) {
            if (checkValidity) {
                Validator.checkValidModel(model);
            }
            id = Validator.checkValidId(model);
        }
        return id;
    }

    static boolean isNew(final Object model) {
        return getId(model) == null;
    }

    @SuppressWarnings("unchecked")
    static void initCollections(final Object model, final Nest<?> nest) {
        if (model == null || nest == null) {
            return;
        }
        
        ModelMetaData metaDataOfClass = JOhm.models.get(model.getClass().getSimpleName());
        Collection<Field> fields = new ArrayList<Field>();
        boolean isCollectionList = false;
        boolean isCollectionSet = false;
        boolean isCollectionSortedSet = false;
        boolean isCollectionMap = false;
        if (metaDataOfClass != null) {
        	 fields = metaDataOfClass.allFields.values();
        }else{
        	fields = gatherAllFields(model.getClass());
        }

        String fieldNameForCache = null;
        for (Field field : fields) {
        	field.setAccessible(true);
        	fieldNameForCache = field.getName();
        	if (metaDataOfClass != null) {
        		isCollectionList = metaDataOfClass.collectionListFields.containsKey(fieldNameForCache);
        		isCollectionSet = metaDataOfClass.collectionSetFields.containsKey(fieldNameForCache);
        		isCollectionSortedSet = metaDataOfClass.collectionSortedSetFields.containsKey(fieldNameForCache);
        		isCollectionMap = metaDataOfClass.collectionMapFields.containsKey(fieldNameForCache);
        	}else{
        		isCollectionList = field.isAnnotationPresent(CollectionList.class);
        		isCollectionSet = field.isAnnotationPresent(CollectionSet.class);
        		isCollectionSortedSet = field.isAnnotationPresent(CollectionSortedSet.class);
        		isCollectionMap = field.isAnnotationPresent(CollectionMap.class);
        	}
        	try {
        		if (isCollectionList) {
        			Validator.checkValidCollection(field);
        			List<Object> list = (List<Object>) field.get(model);
        			if (list == null) {
        				CollectionList annotation = field
        						.getAnnotation(CollectionList.class);
        				RedisList<Object> redisList = new RedisList<Object>(
        						annotation.of(), nest, field, model);
        				field.set(model, redisList);
        			}
        		}
        		if (isCollectionSet) {
        			Validator.checkValidCollection(field);
        			Set<Object> set = (Set<Object>) field.get(model);
        			if (set == null) {
        				CollectionSet annotation = field
        						.getAnnotation(CollectionSet.class);
        				RedisSet<Object> redisSet = new RedisSet<Object>(
        						annotation.of(), nest, field, model);
        				field.set(model, redisSet);
        			}
        		}
        		if (isCollectionSortedSet) {
        			Validator.checkValidCollection(field);
        			Set<Object> sortedSet = (Set<Object>) field.get(model);
        			if (sortedSet == null) {
        				CollectionSortedSet annotation = field
        						.getAnnotation(CollectionSortedSet.class);
        				RedisSortedSet<Object> redisSortedSet = new RedisSortedSet<Object>(
        						annotation.of(), annotation.by(), nest, field,
        						model);
        				field.set(model, redisSortedSet);
        			}
        		}
        		if (isCollectionMap) {
        			Validator.checkValidCollection(field);
        			Map<Object, Object> map = (Map<Object, Object>) field
        					.get(model);
        			if (map == null) {
        				CollectionMap annotation = field
        						.getAnnotation(CollectionMap.class);
        				RedisMap<Object, Object> redisMap = new RedisMap<Object, Object>(
        						annotation.key(), annotation.value(), nest,
        						field, model);
        				field.set(model, redisMap);
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
    }

    static void loadId(final Object model, final Long id) {
        if (model != null) {
            boolean idFieldPresent = false;
            ModelMetaData metaDataOfClass = JOhm.models.get(model.getClass().getSimpleName());
            Collection<Field> fields = new ArrayList<Field>();
            boolean isIdField = false;
            if (metaDataOfClass != null) {
            	fields = metaDataOfClass.allFields.values();
           
            }else{
            	fields = gatherAllFields(model.getClass());
            }
            String fieldNameForCache = null;
            for (Field field : fields) {
            	field.setAccessible(true);
            	fieldNameForCache = field.getName();
            	if (metaDataOfClass != null) {
            		isIdField = metaDataOfClass.idField.equals(fieldNameForCache);
            	}else{
            		isIdField =field.isAnnotationPresent(Id.class);
            	}

            	if (isIdField) {
            		idFieldPresent = true;
            		Validator.checkValidIdType(field);
            		try {
            			field.set(model, id);
            		} catch (IllegalArgumentException e) {
            			throw new JOhmException(e,
            					JOhmExceptionMeta.ILLEGAL_ARGUMENT_EXCEPTION);
            		} catch (IllegalAccessException e) {
            			throw new JOhmException(e,
            					JOhmExceptionMeta.ILLEGAL_ACCESS_EXCEPTION);
            		}
            		break;
            	}
            }

            if (!idFieldPresent) {
                throw new JOhmException(
                        "JOhm does not support a Model without an Id",
                        JOhmExceptionMeta.MISSING_MODEL_ID);
            }
        }
    }

    static boolean detectJOhmCollection(final Field field) {
        boolean isJOhmCollection = false;
        ModelMetaData metaDataOfClass = JOhm.models.get(field.getClass().getSimpleName());
        if (metaDataOfClass != null) {
        	if (metaDataOfClass.collectionListFields.containsKey(field.getName())
        			|| metaDataOfClass.collectionSetFields.containsKey(field.getName())
        			|| metaDataOfClass.collectionSortedSetFields.containsKey(field.getName())
        			|| metaDataOfClass.collectionMapFields.containsKey(field.getName())) {
        		isJOhmCollection = true;
        	}
        }else{
        	if (field.isAnnotationPresent(CollectionList.class)
        			|| field.isAnnotationPresent(CollectionSet.class)
        			|| field.isAnnotationPresent(CollectionSortedSet.class)
        			|| field.isAnnotationPresent(CollectionMap.class)) {
        		isJOhmCollection = true;
        	}
        }
        return isJOhmCollection;
    }

    public static JOhmCollectionDataType detectJOhmCollectionDataType(
            final Class<?> dataClazz) {
        JOhmCollectionDataType type = null;
        if (Validator.checkSupportedPrimitiveClazz(dataClazz)) {
            type = JOhmCollectionDataType.PRIMITIVE;
        } else {
            try {
                Validator.checkValidModelClazz(dataClazz);
                type = JOhmCollectionDataType.MODEL;
            } catch (JOhmException exception) {
                // drop it
            }
        }

        if (type == null) {
            throw new JOhmException(dataClazz.getSimpleName()
                    + " is not a supported JOhm Collection Data Type",
                    JOhmExceptionMeta.UNSUPPORTED_JOHM_COLLECTION);
        }

        return type;
    }

    @SuppressWarnings("unchecked")
    public static boolean isNullOrEmpty(final Object obj) {
        if (obj == null) {
            return true;
        }
          if (obj.getClass().getName().equals(RedisList.class.getName())
        		|| obj.getClass().getName().equals(RedisSet.class.getName())
        		|| obj.getClass().getName().equals(RedisSortedSet.class.getName())) {
            return ((Collection) obj).size() == 0;
        } else if (obj.getClass().getName().equals(RedisMap.class.getName())) {
        	return ((Map) obj).size() == 0;
        }else {
            if (obj.toString().trim().length() == 0) {
                return true;
            }
        }

        return false;
    }

    public static List<Field> gatherAllFields(Class<?> clazz) {
        List<Field> allFields = new ArrayList<Field>();
        for (Field field : clazz.getDeclaredFields()) {
            allFields.add(field);
        }
        while ((clazz = clazz.getSuperclass()) != null) {
            allFields.addAll(gatherAllFields(clazz));
        }

        return Collections.unmodifiableList(allFields);
    }

    public static enum JOhmCollectionDataType {
        PRIMITIVE, MODEL;
    }

    public final static class Convertor {
        static Object convert(final Field field, final String value) {
            return convert(field.getType(), value);
        }

        public static Object convert(final Class<?> type, final String value) {
            if (type.equals(Byte.class) || type.equals(byte.class)) {
                return new Byte(value);
            }
            if (type.equals(Character.class) || type.equals(char.class)) {
                if (!isNullOrEmpty(value)) {
                    if (value.length() > 1) {
                        throw new IllegalArgumentException(
                                "Non-character value masquerading as characters in a string");
                    }
                    return value.charAt(0);
                } else {
                    // This is the default value
                    return '\u0000';
                }
            }
            if (type.equals(Short.class) || type.equals(short.class)) {
                return new Short(value);
            }
            if (type.equals(Integer.class) || type.equals(int.class)) {
                if (value == null) {
                    return 0;
                }
                return new Integer(value);
            }
            if (type.equals(Float.class) || type.equals(float.class)) {
                if (value == null) {
                    return 0f;
                }
                return new Float(value);
            }
            if (type.equals(Double.class) || type.equals(double.class)) {
                return new Double(value);
            }
            if (type.equals(Long.class) || type.equals(long.class)) {
                return new Long(value);
            }
            if (type.equals(Boolean.class) || type.equals(boolean.class)) {
                return new Boolean(value);
            }

            // Higher precision folks
            if (type.equals(BigDecimal.class)) {
                return new BigDecimal(value);
            }
            if (type.equals(BigInteger.class)) {
                return new BigInteger(value);
            }

            if (type.isEnum() || type.equals(Enum.class)) {
                // return Enum.valueOf(type, value);
                return null; // TODO: handle these
            }

            // Raw Collections are unsupported
            if (type.equals(Collection.class)) {
                return null;
            }

            // Raw arrays are unsupported
            if (type.isArray()) {
                return null;
            }

            return value;
        }
    }

    static final class Validator {
        static void checkValidAttribute(final Field field) {
            Class<?> type = field.getType();
            if ((type.equals(Byte.class) || type.equals(byte.class))
                    || type.equals(Character.class) || type.equals(char.class)
                    || type.equals(Short.class) || type.equals(short.class)
                    || type.equals(Integer.class) || type.equals(int.class)
                    || type.equals(Float.class) || type.equals(float.class)
                    || type.equals(Double.class) || type.equals(double.class)
                    || type.equals(Long.class) || type.equals(long.class)
                    || type.equals(Boolean.class) || type.equals(boolean.class)
                    || type.equals(BigDecimal.class)
                    || type.equals(BigInteger.class)
                    || type.equals(String.class)) {
            } else {
                throw new JOhmException(field.getType().getSimpleName()
                        + " is not a JOhm-supported Attribute",
                        JOhmExceptionMeta.UNSUPPORTED_JOHM_ATTRIBUTE);
            }
        }
        
        static void checkValidRangeIndexedAttribute(final Field field) {
        	Class<?> type = field.getType();
        	if ((type.equals(Integer.class) || type.equals(int.class)
                    || type.equals(Float.class) || type.equals(float.class)
                    || type.equals(Double.class) || type.equals(double.class)
                    || type.equals(Long.class) || type.equals(long.class)
                    || type.equals(BigDecimal.class)
                    || type.equals(BigInteger.class))) {
            } else{
            	throw new JOhmException(field.getType().getSimpleName()
                        + " is not a supported type for Comparable annotation",
                        JOhmExceptionMeta.INVALID_MODEL_ANNOTATION);
            }
        }

        static void checkValidReference(final Field field) {
            if (!field.getType().getClass().isInstance(Model.class)) {
                throw new JOhmException(field.getType().getSimpleName()
                        + " is not a subclass of Model",
                        JOhmExceptionMeta.MISSING_MODEL_ANNOTATION);
            }
        }

        static Long checkValidId(final Object model) {
            Long id = null;
            boolean idFieldPresent = false;
            boolean isIdField = false;
            ModelMetaData metaDataOfClass = JOhm.models.get(model.getClass().getSimpleName());
            String fieldNameForCache = null;
            for (Field field : model.getClass().getDeclaredFields()) {
            	field.setAccessible(true);
            	fieldNameForCache = field.getName();
            	if (metaDataOfClass != null) {
            		isIdField = metaDataOfClass.idField.equals(fieldNameForCache);
            	}else {
            		isIdField =  field.isAnnotationPresent(Id.class);
            	}
            	if (isIdField) {
            		Validator.checkValidIdType(field);
            		try {
            			id = (Long) field.get(model);
            			idFieldPresent = true;
            		} catch (IllegalArgumentException e) {
            			throw new JOhmException(e,
            					JOhmExceptionMeta.ILLEGAL_ARGUMENT_EXCEPTION);
            		} catch (IllegalAccessException e) {
            			throw new JOhmException(e,
            					JOhmExceptionMeta.ILLEGAL_ACCESS_EXCEPTION);
            		}
            		break;
            	}
            }
            if (!idFieldPresent) {
                throw new JOhmException(
                        "JOhm does not support a Model without an Id",
                        JOhmExceptionMeta.MISSING_MODEL_ID);
            }
            return id;
        }

        static void checkValidIdType(final Field field) {
        	ModelMetaData metaDataOfClass = JOhm.models.get(field.getClass().getSimpleName());
        	Annotation[] annotations =  null;
        	if (metaDataOfClass != null) {
        		annotations = metaDataOfClass.annotatedFields.get(field.getName());
        	}else{
        		annotations = field.getAnnotations();
        	}
            if (annotations != null && annotations.length > 1) {
                for (Annotation annotation : annotations) {
                    Class<?> annotationType = annotation.annotationType();
                    if (annotationType.equals(Id.class)) {
                        continue;
                    }
                    if (JOHM_SUPPORTED_ANNOTATIONS.contains(annotationType)) {
                        throw new JOhmException(
                                "Element annotated @Id cannot have any other JOhm annotations",
                                JOhmExceptionMeta.INVALID_MODEL_ID_ANNOTATIONS);
                    }
                }
            }
            Class<?> type = field.getType().getClass();
            if (!type.isInstance(Long.class) || !type.isInstance(long.class)) {
                throw new JOhmException(field.getType().getSimpleName()
                        + " is annotated an Id but is not a long",
                        JOhmExceptionMeta.INVALID_MODEL_ID_TYPE);
            }
        }
        
        static boolean isIndexable(final String attributeName) {
            // Prevent null/empty keys and null/empty values
            if (!isNullOrEmpty(attributeName)) {
                return true;
            } else {
                return false;
            }
        }

        static void checkValidModel(final Object model) {
            checkValidModelClazz(model.getClass());
        }

        static void checkValidModelClazz(final Class<?> modelClazz) {
        	ModelMetaData metaDataOfClass = JOhm.models.get(modelClazz.getSimpleName());
        	if (metaDataOfClass == null) {
        		if (!modelClazz.isAnnotationPresent(Model.class)) {
        			throw new JOhmException(
        					"Class pretending to be a Model but is not really annotated",
        					JOhmExceptionMeta.MISSING_MODEL_ANNOTATION);
        		}
        	}
            if (modelClazz.isInterface()) {
                throw new JOhmException(
                        "An interface cannot be annotated as a Model",
                        JOhmExceptionMeta.INVALID_MODEL_ANNOTATION);
            }
        }

        static void checkValidCollection(final Field field) {
            boolean isList = false, isSet = false, isMap = false, isSortedSet = false;
            ModelMetaData metaDataOfClass = JOhm.models.get(field.getClass().getSimpleName());
            boolean isCollectionList = false;
            boolean isCollectionSet = false;
            boolean isCollectionSortedSet = false;
            boolean isCollectionMap = false;
            if (metaDataOfClass != null) {
            	isCollectionList = metaDataOfClass.collectionListFields.containsKey(field.getName());
            	isCollectionSet = metaDataOfClass.collectionSetFields.containsKey(field.getName());
            	isCollectionSortedSet = metaDataOfClass.collectionSortedSetFields.containsKey(field.getName());
            	isCollectionMap = metaDataOfClass.collectionMapFields.containsKey(field.getName());
            }else{
            	isCollectionList = field.isAnnotationPresent(CollectionList.class);
            	isCollectionSet = field.isAnnotationPresent(CollectionSet.class);
            	isCollectionSortedSet = field.isAnnotationPresent(CollectionSortedSet.class);
            	isCollectionMap = field.isAnnotationPresent(CollectionMap.class);
            }
            
            if (isCollectionList) {
            	checkValidCollectionList(field);
            	isList = true;
            }
            if (isCollectionSet) {
            	checkValidCollectionSet(field);
            	isSet = true;
            }
            if (isCollectionSortedSet) {
            	checkValidCollectionSortedSet(field);
            	isSortedSet = true;
            }
            if (isCollectionMap) {
            	checkValidCollectionMap(field);
            	isMap = true;
            }
            
            if (isList && isSet && isMap && isSortedSet) {
                throw new JOhmException(
                        field.getName()
                                + " can be declared a List or a Set or a SortedSet or a Map but not more than one type",
                        JOhmExceptionMeta.INVALID_COLLECTION_ANNOTATION);
            }
        }

        static void checkValidCollectionList(final Field field) {
            if (!field.getType().getClass().isInstance(List.class)) {
                throw new JOhmException(field.getType().getSimpleName()
                        + " is not a subclass of List",
                        JOhmExceptionMeta.INVALID_COLLECTION_SUBTYPE);
            }
        }

        static void checkValidCollectionSet(final Field field) {
            if (!field.getType().getClass().isInstance(Set.class)) {
                throw new JOhmException(field.getType().getSimpleName()
                        + " is not a subclass of Set",
                        JOhmExceptionMeta.INVALID_COLLECTION_SUBTYPE);
            }
        }

        static void checkValidCollectionSortedSet(final Field field) {
            if (!field.getType().getClass().isInstance(Set.class)) {
                throw new JOhmException(field.getType().getSimpleName()
                        + " is not a subclass of Set",
                        JOhmExceptionMeta.INVALID_COLLECTION_SUBTYPE);
            }
        }

        static void checkValidCollectionMap(final Field field) {
            if (!field.getType().getClass().isInstance(Map.class)) {
                throw new JOhmException(field.getType().getSimpleName()
                        + " is not a subclass of Map",
                        JOhmExceptionMeta.INVALID_COLLECTION_SUBTYPE);
            }
        }

        static void checkValidArrayBounds(final Field field, int actualLength) {
            if (field.getAnnotation(Array.class).length() < actualLength) {
                throw new JOhmException(
                        field.getType().getSimpleName()
                                + " has an actual length greater than the expected annotated array bounds",
                        JOhmExceptionMeta.INVALID_ARRAY_BOUNDS);
            }
        }

        static void checkAttributeReferenceIndexRules(final Field field) {
        	ModelMetaData metaDataOfClass = JOhm.models.get(field.getClass().getSimpleName());
        	boolean isAttribute = false;
        	boolean isReference = false;
        	boolean isIndexed = false;
        	if (metaDataOfClass != null) {
        		isAttribute = metaDataOfClass.attributeFields.containsKey(field.getName());
        		isReference = metaDataOfClass.referenceFields.containsKey(field.getName());
        		isIndexed =  metaDataOfClass.indexedFields.containsKey(field.getName());
        	}else{
        		isAttribute = field.isAnnotationPresent(Attribute.class);
        		isReference = field.isAnnotationPresent(Reference.class);
        		isIndexed = field.isAnnotationPresent(Indexed.class);
        	}
            if (isAttribute) {
                if (isReference) {
                    throw new JOhmException(
                            field.getName()
                                    + " is both an Attribute and a Reference which is invalid",
                            JOhmExceptionMeta.INVALID_ATTRIBUTE_AND_REFERENCE);
                }
                if (isIndexed) {
                    if (!isIndexable(field.getName())) {
                    	throw new JOhmException(new InvalidFieldException(),
                                JOhmExceptionMeta.MISSING_INDEXED_ANNOTATION);
                    }
                }
                if (field.getType().equals(Model.class)) {
                    throw new JOhmException(field.getType().getSimpleName()
                            + " is an Attribute and a Model which is invalid",
                            JOhmExceptionMeta.INVALID_ATTRIBUTE_AND_MODEL);
                }
                checkValidAttribute(field);
            }
            if (isReference) {
                checkValidReference(field);
            }
        }
        
        static void checkHashTagRules(final Field field) {
        	ModelMetaData metaDataOfClass = JOhm.models.get(field.getClass().getSimpleName());
        	boolean isAttribute = false;
        	boolean isHashTag = false;
        	if (metaDataOfClass != null) {
        		isAttribute = metaDataOfClass.attributeFields.containsKey(field.getName());
        		isHashTag = metaDataOfClass.hashTaggedFields.containsKey(field.getName());
        	}else{
        		isAttribute = field.isAnnotationPresent(Attribute.class);
        		isHashTag = field.isAnnotationPresent(HashTag.class);
        	}

        	if (isHashTag && !isAttribute) {
        		throw new JOhmException(
        				field.getName()
        				+ " has HashTag annotation but is not attribute which is invalid",
        				JOhmExceptionMeta.INVALID_HASH_TAG);
        	}
        }

        public static boolean checkSupportedPrimitiveClazz(
                final Class<?> primitiveClazz) {
            return JOHM_SUPPORTED_PRIMITIVES.contains(primitiveClazz);
        }
    }

    private static final Set<Class<?>> JOHM_SUPPORTED_PRIMITIVES = new HashSet<Class<?>>();
    private static final Set<Class<?>> JOHM_SUPPORTED_ANNOTATIONS = new HashSet<Class<?>>();
    static {
        JOHM_SUPPORTED_PRIMITIVES.add(String.class);
        JOHM_SUPPORTED_PRIMITIVES.add(Byte.class);
        JOHM_SUPPORTED_PRIMITIVES.add(byte.class);
        JOHM_SUPPORTED_PRIMITIVES.add(Character.class);
        JOHM_SUPPORTED_PRIMITIVES.add(char.class);
        JOHM_SUPPORTED_PRIMITIVES.add(Short.class);
        JOHM_SUPPORTED_PRIMITIVES.add(short.class);
        JOHM_SUPPORTED_PRIMITIVES.add(Integer.class);
        JOHM_SUPPORTED_PRIMITIVES.add(int.class);
        JOHM_SUPPORTED_PRIMITIVES.add(Float.class);
        JOHM_SUPPORTED_PRIMITIVES.add(float.class);
        JOHM_SUPPORTED_PRIMITIVES.add(Double.class);
        JOHM_SUPPORTED_PRIMITIVES.add(double.class);
        JOHM_SUPPORTED_PRIMITIVES.add(Long.class);
        JOHM_SUPPORTED_PRIMITIVES.add(long.class);
        JOHM_SUPPORTED_PRIMITIVES.add(Boolean.class);
        JOHM_SUPPORTED_PRIMITIVES.add(boolean.class);
        JOHM_SUPPORTED_PRIMITIVES.add(BigDecimal.class);
        JOHM_SUPPORTED_PRIMITIVES.add(BigInteger.class);

        JOHM_SUPPORTED_ANNOTATIONS.add(Array.class);
        JOHM_SUPPORTED_ANNOTATIONS.add(Attribute.class);
        JOHM_SUPPORTED_ANNOTATIONS.add(CollectionList.class);
        JOHM_SUPPORTED_ANNOTATIONS.add(CollectionMap.class);
        JOHM_SUPPORTED_ANNOTATIONS.add(CollectionSet.class);
        JOHM_SUPPORTED_ANNOTATIONS.add(CollectionSortedSet.class);
        JOHM_SUPPORTED_ANNOTATIONS.add(Id.class);
        JOHM_SUPPORTED_ANNOTATIONS.add(Indexed.class);
        JOHM_SUPPORTED_ANNOTATIONS.add(Model.class);
        JOHM_SUPPORTED_ANNOTATIONS.add(Reference.class);
    }
}
