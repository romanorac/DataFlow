package com.romelj.dataflow.movies;

import com.google.common.io.Files;
import com.romelj.dataflow.utils.FileOp;
import com.romelj.dataflow.entities.TTestSample;
import com.romelj.dataflow.utils.StatisticsOp;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class Task4Test {

	@Test
	public void runTask() throws Exception {
		URL inputURL = this.getClass().getClassLoader().getResource("datasets/movies");
		if (inputURL != null) {
			File inputFile = new File(inputURL.toURI());
			File outputFile = Files.createTempDir();
			String inputPath = inputFile.getAbsolutePath();
			String outputPath = outputFile.getAbsolutePath() + File.separator;

			Task4.runTask(inputPath, outputPath, true);

			List<TTestSample> usageGenderJsonList = FileOp.readFiles(outputPath, TTestSample.class);
			assertEquals(2, usageGenderJsonList.size());

			TTestSample tTestSample1 = usageGenderJsonList.get(0);
			TTestSample tTestSample2 = usageGenderJsonList.get(1);

			double tValue = StatisticsOp.calcTValue(tTestSample1.getMean(), tTestSample1.getVariance(), tTestSample1.getSamples(), tTestSample2.getMean(), tTestSample2.getVariance(), tTestSample2.getSamples());
			assertEquals(0.30367584720044977, tValue, 0);
		}
	}

}