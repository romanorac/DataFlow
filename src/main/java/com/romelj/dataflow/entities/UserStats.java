package com.romelj.dataflow.entities;

/**
 * Used in movies task 4 to store mean of movie scores by user and number of samples.
 */
public class UserStats implements User {
	private final String userId;
	private final String gender;
	private final double score;
	private final int numEvents;

	public UserStats(String userId, String gender, double score, int numEvents) {
		this.userId = userId;
		this.gender = gender;
		this.score = score;
		this.numEvents = numEvents;
	}

	public String getUserId() {
		return userId;
	}

	public String getGender() {
		return gender;
	}

	public double getScore() {
		return score;
	}

	public int getNumEvents() {
		return numEvents;
	}
}
