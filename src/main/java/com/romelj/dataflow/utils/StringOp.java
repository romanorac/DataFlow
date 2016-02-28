package com.romelj.dataflow.utils;

import com.romelj.dataflow.entities.User;

/**
 * String operations
 */
public class StringOp {

	/**
	 * check if string is not empty
	 *
	 * @param input - some string
	 * @return - true if string is not empty
	 */
	static boolean checkIfNotEmpty(String input) {
		return input != null && input.length() > 0;
	}

	/**
	 * check if gender param is m or f
	 *
	 * @param input - some string
	 * @return - true if gender is m or f
	 */
	static boolean checkGender(String input) {
		return checkIfNotEmpty(input) && (input.equals("m") || input.equals("f"));
	}

	/**
	 * Check if string is an integer number
	 * @param input - some string
	 * @return true, if string is a number
	 */
	private static boolean checkIfInteger(String input) {
		return checkIfNotEmpty(input) && input.trim().matches("\\d+");
	}

	/**
	 * check if integer is a positive integer
	 * @param input - some string
	 * @return true, if integer is a positive integer
	 */
	static boolean checkIfPosInteger(String input) {
		try {
			return checkIfInteger(input) && Integer.parseInt(input.trim()) >= 0;
		}catch ( NumberFormatException e){
			return false;
		}
	}

	/**
	 * Check if string is positive long
	 *
	 * @param input - some string
	 *
	 * @return - true if string can be parsed to pos long
	 */
	static boolean checkIfPosLong(String input) {
		try {
			return checkIfInteger(input) && Long.parseLong(input.trim()) >= 0;
		}catch ( NumberFormatException e){
			return false;
		}
	}

	/**
	 * Get a gender from user events
	 *
	 * @param userIterable - grouped user events
	 * @return - gender or "null"
	 */
	public static String getGender(Iterable<User> userIterable) {
		String defaultValue = "null";
		String gender = defaultValue;
		if (userIterable != null) {
			for (User user : userIterable) {
				if (gender.equals(defaultValue) && user.getGender() != null) {
					gender = user.getGender();
					break;
				}
			}
		}
		return gender;
	}

	/**
	 * Check if path if gcs path
	 * @param input - some string path
	 * @return - true, if path starts with gs
	 */
	public static boolean checkGcsPath(String input) {
		return checkIfNotEmpty(input) && input.startsWith("gs://");
	}

}
