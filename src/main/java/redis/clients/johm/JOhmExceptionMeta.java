package redis.clients.johm;

/**
 * JOhm error dictionary to aggregate all JOhmException metadata in one place for
 * allowing clients to setup programmatic response tactics and also for easy reference. 
 */
public enum JOhmExceptionMeta {
    GENERIC_EXCEPTION("Generic catch-all exception"),
    INSTANTIATION_EXCEPTION("Instantiation exception"),
    ILLEGAL_ACCESS_EXCEPTION("Illegal Access exception"),
    ILLEGAL_ARGUMENT_EXCEPTION("Illegal Argument exception"),
    SECURITY_EXCEPTION("Security exception"),
    NO_SUCH_FIELD_EXCEPTION("No Such Field exception"),
    NULL_JEDIS_POOL("Injected JedisPool is null"),
    UNSUPPORTED_JOHM_COLLECTION("Unsupported JOhm Collection datatype"),
    UNSUPPORTED_JOHM_ATTRIBUTE("Unsupported JOhm Attribute datatype"),
    MISSING_MODEL_ID("Model is missing its Id"),
    INVALID_MODEL_ID_TYPE("Model Id is not of long type"),
    INVALID_MODEL_ID_ANNOTATIONS("Field annotated Id cannot have any other JOhm annotations"),
    MISSING_MODEL_ANNOTATION("Class pretending to be Model does not have Model annotation"),
    MISSING_INDEXED_ANNOTATION("Field pretending to be indexed does not have Indexed annotation"),
    MISSING_COMPARABLE_ANNOTATION("Field pretending to be comparable does not have Comparable annotation"),
    INVALID_MODEL_ANNOTATION("Interface cannot be annotated as a Model"),
    INVALID_ATTRIBUTE_AND_REFERENCE("Field is both an Attribute and Reference which is invalid"),
    INVALID_HASH_TAG("Field is HashTag and is not an attribute"),
    INVALID_ATTRIBUTE_AND_MODEL("Field is both an Attribute and Model which is invalid"),
    INVALID_VALUE("Value provided is not valid, it is either null or empty"),
    INVALID_INDEX("Field is not indexable based on type"),
    INVALID_COLLECTION_SUBTYPE("Field is invalid subtype of its corresponding Collection super-interface"),
    INVALID_COLLECTION_ANNOTATION("Field has invalid Collection annotations"),
    NULL_OR_EMPTY_VALUE_HASH_TAG("Field is HashTag and has null or empty value"),
    INVALID_ARRAY_BOUNDS("Field has an actual length greater that annotated array bound");

    private final String message;

    private JOhmExceptionMeta(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
