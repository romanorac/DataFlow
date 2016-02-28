package com.romelj.dataflow.utils;

import com.romelj.dataflow.entities.User;

public class StatisticsOp {

	/**
	 * Calculate mean from user events
	 *
	 * @param userStatsIterable - grouped user events
	 * @return Mean objects with mean and number of samples
	 */
	public static Mean calcMean(Iterable<User> userStatsIterable) {
		int numSamples = 0;
		double mean = 0;
		for (User user : userStatsIterable) {
			mean += user.getScore();
			numSamples++;
		}

		if (numSamples > 0) {
			mean = mean / numSamples;
		}

		return new Mean(mean, numSamples);
	}

	/**
	 * Calculate variance from user events
	 *
	 * @param userStatsIterable - grouped user events
	 * @param mean - value of mean
	 * @param numSamples - number of samples taken in mean
	 * @return variance
	 */
	public static double calcVariance(Iterable<User> userStatsIterable, double mean, int numSamples) {
		double variance = 0;
		if (mean != 0 && userStatsIterable != null) {
			for (User userStats : userStatsIterable) {
				variance += Math.pow(userStats.getScore() - mean, 2);
			}
			if(numSamples - 1 != 0) {
				//The Population: divide by N when calculating Variance, A Sample: divide by N-1 when calculating Variance.
				variance /= (numSamples - 1);
			}

		}
		return variance;
	}

	/**
	 * calculate t value with t-test
	 *
	 * @param xMean - mean of first sample
	 * @param xVar - variance of first sample
	 * @param xSamples - number of samples in first sample
	 * @param yMean - mean of second sample
	 * @param yVar - variance of second sample
	 * @param ySamples - number of samples in second sample
	 * @return t-value
	 */
	public static double calcTValue(double xMean, double xVar, int xSamples, double yMean, double yVar, int ySamples) {
		double tValue = 0;
		if (xSamples > 0 && ySamples > 0) {
			//we use abs on mean1 and mean2 as we do not know the order
			tValue = Math.abs(xMean - yMean) / Math.sqrt(xVar / xSamples + yVar / ySamples);
		}

		return tValue;

	}

	/**
	 * Mean object stores mean and the number of samples that mean was calculated from.
	 */
	public static class Mean {
		private final double mean;
		private final int numSamples;

		public Mean(double mean, int numSamples) {
			this.mean = mean;
			this.numSamples = numSamples;
		}

		public double getMean() {
			return mean;
		}

		public int getNumSamples() {
			return numSamples;
		}
	}


}
