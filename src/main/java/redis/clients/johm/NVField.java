package redis.clients.johm;

/**
 * NVPair provides a helper-class for providing the attribute name, values and condition to apply.
 * It allows queries for attributes
 */
public class NVField {
	private String attributeName;
	private String referenceAttributeName;
	private Object referenceAttributeValue;
	private Object attributeValue;
	private Condition conditionUsed;
	
	public static enum Condition {
        EQUALS, GREATERTHANEQUALTO, LESSTHANEQUALTO, GREATERTHAN, LESSTHAN, NOTEQUALS;
	}

	public NVField(String attributeName, Object attributeValue) {
		this.attributeName=attributeName;
		this.attributeValue = attributeValue;
		this.conditionUsed = Condition.EQUALS;
	}

	public NVField(String attributeName, String referenceAttributeName, Object referenceAttributeValue) {
		this.attributeName = attributeName;
		this.referenceAttributeName = referenceAttributeName;
		this.referenceAttributeValue = referenceAttributeValue;
		this.conditionUsed = Condition.EQUALS;
	}
	
	public NVField(String attributeName, Object attributeValue, Condition operator) {
		this.attributeName=attributeName;
		this.attributeValue = attributeValue;
		this.conditionUsed = operator;
	}

	public NVField(String attributeName, String referenceAttributeName, Object referenceAttributeValue, Condition operator) {
		this.attributeName = attributeName;
		this.referenceAttributeName = referenceAttributeName;
		this.referenceAttributeValue = referenceAttributeValue;
		this.conditionUsed = operator;
	}

	/**
	 * Get the attribute name
	 */
	 public String getAttributeName() {
		return(attributeName);
	 }

	 /**
	  * Get the attribute values
	  */
	 public Object getAttributeValue() {
		 return(attributeValue);
	 }

	 /**
	  * Get reference attribute name
	  * @return String
	  */
	 public String getReferenceAttributeName() {
		 return referenceAttributeName;
	 }

	 /**
	  * Get reference attribute value
	  * @return String
	  */
	 public Object getReferenceAttributeValue() {
		 return referenceAttributeValue;
	 }

	/**
	 * Condition used.
	 * 
	 * @return Operand
	 */
	public Condition getConditionUsed() {
		return conditionUsed;
	}
}
