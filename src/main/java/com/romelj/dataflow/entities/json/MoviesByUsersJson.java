package com.romelj.dataflow.entities.json;

/**
 * Json entity, used in movie Task1, stores the mean of movies that users watch
 */
public class MoviesByUsersJson {
	private double meanMoviesWatch;

	public MoviesByUsersJson() {
	}

	public MoviesByUsersJson(double meanMoviesWatch) {
		this.meanMoviesWatch = meanMoviesWatch;
	}

	public double getMeanMoviesWatch() {
		return meanMoviesWatch;
	}

	public String toString() {
		return String.format("meanMoviesWatch: %.2f", meanMoviesWatch);
	}
}
