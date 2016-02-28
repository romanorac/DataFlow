package com.romelj.dataflow.entities.json;

/**
 * Json entity used in tutorial task1
 */
public class DurationCountJson {

	private final long durationMinutes;
	private final long numSessions;

	public DurationCountJson(long durationMinutes, long numSessions) {
		this.durationMinutes = durationMinutes;
		this.numSessions = numSessions;
	}

	public long getDurationMinutes() {
		return durationMinutes;
	}

	public long getNumSessions() {
		return numSessions;
	}

	public String toString() {
		return "durationMinutes: " + String.valueOf(durationMinutes) + ", numSessions: " + numSessions;
	}
}
