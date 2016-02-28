package com.romelj.dataflow;

public class Settings {

	public static final String delimiter = ";"; //delimiter of input files

	public static final String projectId = "INPUT_YOUR_PROJECT_ID";

	//change google cloud storage locations
	public static final String stagingLocation = "gs://dataflow/staging";

	public static final String moviesInput = "gs://dataflow/input/movie_data/";
	public static final String moviesOutput = "gs://dataflow/output/movie_data";

	public static final String tutorialInput = "gs://dataflow/input/tutorial_data/";
	public static final String tutorialOutput = "gs://dataflow/output/tutorial_data";

}
