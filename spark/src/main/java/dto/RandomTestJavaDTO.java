package dto;

import java.sql.Timestamp;

public class RandomTestJavaDTO {
	private final String uuid;
	private final String position;
	private final Integer age;
	private final Timestamp time;
	private final Double prob;
	private final Integer uses;

	public RandomTestJavaDTO(String uuid, String position, Integer age, Timestamp time, Double prob, Integer uses) {
		this.uuid = uuid;
		this.position = position;
		this.age = age;
		this.time = time;
		this.prob = prob;
		this.uses = uses;
	}

	public String uuid() {
		return uuid;
	}

	public String position() {
		return position;
	}

	public Integer age() {
		return age;
	}

	public Timestamp time() {
		return time;
	}

	public Double prob() {
		return prob;
	}

	public Integer uses() {
		return uses;
	}
}