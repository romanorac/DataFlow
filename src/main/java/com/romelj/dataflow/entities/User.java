package com.romelj.dataflow.entities;

import java.io.Serializable;


/**
 * we used interface to be able to calculate mean and variance for UserEvents and UserStats
 */
public interface User extends Serializable {

	double getScore();

	String getGender();
}
