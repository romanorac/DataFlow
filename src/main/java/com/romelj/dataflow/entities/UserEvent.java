package com.romelj.dataflow.entities;

/**
 * Used in Movie tasks to extract the data. It presents some overhead as some tasks does not require all data.
 */
public class UserEvent implements User {
	private final String id;
	private final String gender;
	private final String movieId;
	private final double score;

	public UserEvent(String id, String gender, String movieId, int score) {
		this.id = id;
		this.gender = gender;
		this.movieId = movieId;
		this.score = score;
	}

	public String getId() {
		return id;
	}

	public String getGender() {
		return gender;
	}

	public String getMovieId() {
		return movieId;
	}

	public double getScore() {
		return score;
	}
}