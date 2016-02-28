package com.romelj.dataflow.entities;

import java.io.Serializable;

/**
 * TTestSample groups users by gender and stores their mean, variance and number of samples that we need to calculate the t-value with t test.
 */
public class TTestSample implements Serializable {
	private final String gender;
	private final double mean;
	private final double variance;
	private final int samples;

	public TTestSample(String gender, double mean, double variance, int samples) {
		this.gender = gender;
		this.mean = mean;
		this.variance = variance;
		this.samples = samples;
	}

	public String getGender() {
		return gender;
	}

	public double getMean() {
		return mean;
	}

	public double getVariance() {
		return variance;
	}

	public int getSamples() {
		return samples;
	}

	public String toString() {
		return String.format("gender: %s, mean: %.2f, var: %.2f, numSamples: %d\n", gender, mean, variance, samples);
	}
}
