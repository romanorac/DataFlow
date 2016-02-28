package com.romelj.dataflow.movies;

import com.google.common.io.Files;
import com.romelj.dataflow.utils.FileOp;
import com.romelj.dataflow.entities.json.MoviesByUsersJson;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class Task1Test {

	@Test
	public void runTask() throws Exception {
		URL inputURL = this.getClass().getClassLoader().getResource("datasets/movies");
		if (inputURL != null) {
			File inputFile = new File(inputURL.toURI());
			File outputFile = Files.createTempDir();
			String inputPath = inputFile.getAbsolutePath();
			String outputPath = outputFile.getAbsolutePath() + File.separator;

			Task1.runTask(inputPath, outputPath, true);
			List<MoviesByUsersJson> moviesByUsersJsonList = FileOp.readFiles(outputPath, MoviesByUsersJson.class);

			assertEquals(1, moviesByUsersJsonList.size());
			assertEquals(1.5454545454545454, moviesByUsersJsonList.get(0).getMeanMoviesWatch(), 0);
		}
	}
}