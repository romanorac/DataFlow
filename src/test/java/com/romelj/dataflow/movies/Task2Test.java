package com.romelj.dataflow.movies;

import com.google.common.io.Files;
import com.romelj.dataflow.utils.FileOp;
import com.romelj.dataflow.entities.json.UsageGenderJson;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class Task2Test {

	@Test
	public void runTask() throws Exception {
		URL inputURL = this.getClass().getClassLoader().getResource("datasets/movies");
		if (inputURL != null) {
			File inputFile = new File(inputURL.toURI());
			File outputFile = Files.createTempDir();
			String inputPath = inputFile.getAbsolutePath();
			String outputPath = outputFile.getAbsolutePath() + File.separator;

			Task2.runTask(inputPath, outputPath, true);

			List<UsageGenderJson> usageGenderJsonList = FileOp.readFiles(outputPath, UsageGenderJson.class);
			assertEquals(2, usageGenderJsonList.size());

			for(UsageGenderJson usageGenderJson : usageGenderJsonList) {
				switch (usageGenderJson.getGender()) {
					case "m":
						assertEquals(0.6363636363636364, usageGenderJson.getPtc(), 0);
						break;
					case "f":
						assertEquals(0.36363636363636365, usageGenderJson.getPtc(), 0);
						break;
					default:
						throw new Exception("Gender not expected");
				}
			}
		}
	}

}