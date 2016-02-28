package com.romelj.dataflow;

import com.romelj.dataflow.movies.Task1;
import com.romelj.dataflow.movies.Task2;
import com.romelj.dataflow.movies.Task3;
import com.romelj.dataflow.movies.Task4;
import com.romelj.dataflow.utils.StringOp;

import java.io.IOException;
import java.net.URISyntaxException;

public class Main {

	private static void runTutorialTask1(String input, String output) throws IOException, URISyntaxException {
		if (StringOp.checkGcsPath(input) && StringOp.checkGcsPath(output)) {
			com.romelj.dataflow.tutorial.Task1.runTask(input, output, false);
		} else {
			System.out.println("Input or output path is incorrect.");
		}
	}

	private static void runMoviesTask1(String input, String output) throws IOException, URISyntaxException {
		if (StringOp.checkGcsPath(input) && StringOp.checkGcsPath(output)) {
			Task1.runTask(input, output, false);
		} else {
			System.out.println("Input or output path is incorrect.");
		}
	}

	private static void runMoviesTask2(String input, String output) throws IOException, URISyntaxException {
		if (StringOp.checkGcsPath(input) && StringOp.checkGcsPath(output)) {
			Task2.runTask(input, output, false);
		} else {
			System.out.println("Input or output path is incorrect.");
		}
	}

	private static void runMoviesTask3(String input, String output) throws IOException, URISyntaxException {
		if (StringOp.checkGcsPath(input) && StringOp.checkGcsPath(output)) {
			Task3.runTask(input, output, false);
		} else {
			System.out.println("Input or output path is incorrect.");
		}
	}

	private static void runMoviesTask4(String input, String output) throws IOException, URISyntaxException {
		if (StringOp.checkGcsPath(input) && StringOp.checkGcsPath(output)) {
			Task4.runTask(input, output, false);
		} else {
			System.out.println("Input or output path is incorrect.");
		}
	}

	/**
	 *Run tasks on cloud DataFlow
	 */
	public static void main(String[] args) throws IOException, URISyntaxException {
		//run movie tasks
//		runMoviesTask1(Settings.moviesInput, Settings.moviesOutput);
//		runMoviesTask2(Settings.moviesInput, Settings.moviesOutput);
//		runMoviesTask3(Settings.moviesInput, Settings.moviesOutput);
		runMoviesTask4(Settings.moviesInput, Settings.moviesOutput);

		//run tutorial task
//		runTutorialTask1(Settings.tutorialInput, Settings.tutorialOutput);


	}
}
