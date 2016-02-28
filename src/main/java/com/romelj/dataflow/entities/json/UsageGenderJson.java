package com.romelj.dataflow.entities.json;

public class UsageGenderJson {

	private final String gender;
	private final double ptc;

	public UsageGenderJson(String gender, double ptc) {
		this.gender = gender;
		this.ptc = ptc;
	}

	public String getGender() {
		return gender;
	}

	public double getPtc() {
		return ptc;
	}

	public String toString() {
		return String.format("gender: %s, ptc: %.2f", gender, ptc);
	}
}
