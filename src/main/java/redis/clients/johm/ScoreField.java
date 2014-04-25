package redis.clients.johm;

/**
 * ScoreField provides a helper-class for providing the score associated with a member.
 * It is used for sorted sets.
 */
public class ScoreField {
	private String key;
	private Double score;
		
	public ScoreField(String key, Double score) {
		this.score=score;
		this.key = key;
	}
	
	public String getKey() {
		return key;
	}
	
	public Double getScore() {
		return score;
	}	
}
