package com.romelj.dataflow.tutorial;

import com.google.common.io.Files;
import com.romelj.dataflow.utils.FileOp;
import com.romelj.dataflow.entities.json.DurationCountJson;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class Task1Test {

	@Test
	public void runTask() throws Exception {
		URL inputURL = this.getClass().getClassLoader().getResource("datasets/tutorial");
		if (inputURL != null) {
			File inputFile = new File(inputURL.toURI());
			File outputFile = Files.createTempDir();
			String inputPath = inputFile.getAbsolutePath();
			String outputPath = outputFile.getAbsolutePath() + File.separator;

			Task1.runTask(inputPath, outputPath, true);

			List<DurationCountJson> durationCountJsonList = FileOp.readFiles(outputPath, DurationCountJson.class);
			assertEquals(4, durationCountJsonList.size());

			for(DurationCountJson durationCountJson: durationCountJsonList){
				if(durationCountJson.getDurationMinutes() == 0){
					assertEquals(1, durationCountJson.getNumSessions());
				}else if(durationCountJson.getDurationMinutes() == 1){
					assertEquals(2, durationCountJson.getNumSessions());
				}else if(durationCountJson.getDurationMinutes() == 3){
					assertEquals(1, durationCountJson.getNumSessions());
				}else if(durationCountJson.getDurationMinutes() == 166){
					assertEquals(3, durationCountJson.getNumSessions());
				}else{
					throw new Exception("Duration not expected");
				}

			}

		}

	}
}