package com.romelj.dataflow.utils;

import static com.romelj.dataflow.utils.StringOp.*;


public class FilterOp {

	/**
	 * check if movie event meets condition
	 *
	 * @param entity - split line of text
	 * @return true if entity meets condition
	 */
	public static boolean filterMovieEvents(String[] entity) {
		return entity.length == 4 && checkIfNotEmpty(entity[0]) && checkGender(entity[1]) && checkIfNotEmpty(entity[2]) && checkIfPosInteger(entity[3]);
	}

	/**
	 * check if tutorial event meets condition
	 *
	 * @param entity - split line of text
	 * @return true if entity meets condition
	 */
	public static boolean filterTutorialEvents(String[] entity) {
		return entity.length == 2 && checkIfNotEmpty(entity[0]) && checkIfPosLong(entity[1]);
	}
}
