package com.romelj.dataflow.movies;

import com.google.common.io.Files;
import com.romelj.dataflow.utils.FileOp;
import com.romelj.dataflow.entities.json.UsageGenderJson;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class Task3Test {

	@Test
	public void runTask() throws Exception {
		URL inputURL = this.getClass().getClassLoader().getResource("datasets/movies");
		if (inputURL != null) {
			File inputFile = new File(inputURL.toURI());
			File outputFile = Files.createTempDir();
			String inputPath = inputFile.getAbsolutePath();
			String outputPath = outputFile.getAbsolutePath() + File.separator;

			Task3.runTask(inputPath, outputPath, true);

			List<UsageGenderJson> usageGenderJsonList = FileOp.readFiles(outputPath, UsageGenderJson.class);
			assertEquals(2, usageGenderJsonList.size());

			for(UsageGenderJson usageGenderJson: usageGenderJsonList){
				switch (usageGenderJson.getGender()) {
					case "m":
						assertEquals(1.5714285714285714, usageGenderJson.getPtc(), 0);
						break;
					case "f":
						assertEquals(1.5, usageGenderJson.getPtc(), 0);
						break;
					default:
						throw new Exception("Gender not expected");
				}
			}

		}
	}

}