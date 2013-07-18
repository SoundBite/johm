package redis.clients.johm;

/**
 * NVPair provides a helper-class for providing the attribute name and values.
 * It allows queries for attributes
 */
public class NVField {
	private String attributeName;
	private String referenceAttributeName;
	private Object referenceAttributeValue;
	private Object attributeValue;

	public NVField(String attributeName, Object attributeValue) {
		this.attributeName=attributeName;
		this.attributeValue = attributeValue;
	}

	public NVField(String attributeName, String referenceAttributeName, Object referenceAttributeValue) {
		this.attributeName = attributeName;
		this.referenceAttributeName = referenceAttributeName;
		this.referenceAttributeValue = referenceAttributeValue;
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
}
