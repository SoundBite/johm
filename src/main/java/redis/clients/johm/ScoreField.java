package redis.clients.johm;

/**
 * ScoreField provides a helper-class for providing the score associated with a member.
 * It is used for sorted sets.
 */
public class ScoreField {
	private Double score;
	private String member;

	public ScoreField(Double score, String member) {
		this.score=score;
		this.member =member;
	}

	public Double getScore() {
		return score;
	}

	public String getMember() {
		return member;
	}
}
