package com.romelj.dataflow.movies;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.gson.Gson;
import com.romelj.dataflow.Settings;
import com.romelj.dataflow.dofn.ExtractMoviesData;
import com.romelj.dataflow.transforms.ReadDocuments;
import com.romelj.dataflow.entities.TTestSample;
import com.romelj.dataflow.entities.User;
import com.romelj.dataflow.entities.UserStats;
import com.romelj.dataflow.utils.Options;
import com.romelj.dataflow.utils.StatisticsOp;
import com.romelj.dataflow.utils.StringOp;

import java.io.IOException;
import java.net.URISyntaxException;

import static com.romelj.dataflow.utils.FileOp.listInputDocuments;


/**
 * Perhaps males and females rate movies differently, perhaps males are rating with generally lower ratings than females
 * or perhaps it is the other way around. Is there a difference in ratings between genders,
 * which gender rates movies with higher ratings, is this difference significant?
 */
public class Task4 {

	/**
	 * Run movies Task 4
	 *
	 * @param input - local or gcs input path
	 * @param output - local or gcs output path
	 * @param locally - run task locally or on cloud data flow
	 * @throws IOException - exception at reading files
	 * @throws URISyntaxException - exception at reading files
	 */
	public static void runTask(String input, String output, boolean locally) throws IOException, URISyntaxException {

		if (!Strings.isNullOrEmpty(input) && !Strings.isNullOrEmpty(output)) {
			Options options = PipelineOptionsFactory.as(Options.class);
			options.setJobName(Task4.class.getClass().getSimpleName());
			options.setProject(Settings.projectId);
			options.setInput(input);
			options.setOutput(output);

			if (!locally) {
				//run task on cloud data flow and set runner and staging location
				options.setRunner(BlockingDataflowPipelineRunner.class);
				options.setStagingLocation(Settings.stagingLocation);
			}

			Pipeline p = Pipeline.create(options);

			// pipeline applies the composite TTestTransform transform, and passes the static FormatAsTextFn() to the ParDo transform.
			p.apply(new ReadDocuments(listInputDocuments(options)))
					.apply("TTestTransform", new Task4.TTestTransform())
					.apply("FormatAsText", MapElements.via(new Task4.FormatAsTextFn()))
					.apply(TextIO.Write.named("WriteTTestSamples").to(options.getOutput()));
			p.run();
		}
	}

	/**
	 * A SimpleFunction that converts TTestSample by gender to json.
	 */
	private static class FormatAsTextFn extends SimpleFunction<KV<String, TTestSample>, String> {

		@Override
		public String apply(KV<String, TTestSample> input) {
			Gson gson = new Gson();
			return gson.toJson(input.getValue());
		}
	}

	/**
	 * DoFn takes user's ratings and calculates mean and number of samples. It outputs KV<gender, userStats>
	 *
	 */
	private static class CalculateUserStats extends DoFn<KV<String, Iterable<User>>, KV<String, User>> {

		@Override
		public void processElement(ProcessContext c) throws Exception {
			String userId = c.element().getKey();
			String gender = StringOp.getGender(c.element().getValue());

			StatisticsOp.Mean mean = StatisticsOp.calcMean(c.element().getValue()); //calculate mean and number of samples

			User userStats = new UserStats(userId, gender, mean.getMean(), mean.getNumSamples());
			c.output(KV.of(gender, userStats));

		}
	}

	/**
	 * DoFn calculates mean and variance for all grouped genders. It outputs KV<gender, TTestSample>
	 */
	private static class CalculateTTestStats extends DoFn<KV<String, Iterable<User>>, KV<String, TTestSample>> {

		@Override
		public void processElement(ProcessContext c) throws Exception {
			String gender = c.element().getKey();
			StatisticsOp.Mean mean = StatisticsOp.calcMean(c.element().getValue());
			double variance = StatisticsOp.calcVariance(c.element().getValue(), mean.getMean(), mean.getNumSamples());
			TTestSample tTestSample = new TTestSample(gender, mean.getMean(), variance, mean.getNumSamples());

			c.output(KV.of(gender, tTestSample));
		}
	}

	/**
	 * A PTransform that converts a PCollection containing lines of text into a PCollection of
	 * mean of movies watched by users
	 */
	private static class TTestTransform extends PTransform<PCollection<String>, PCollection<KV<String, TTestSample>>> {

		@Override
		public PCollection<KV<String, TTestSample>> apply(PCollection<String> lines) {
			//Read the data as KV pairs, where userId is the key and User object is the value.
			PCollection<KV<String, User>> kvpUserIdUser = lines.apply(ParDo.of(new ExtractMoviesData()));

			//group User objects by userId
			PCollection<KV<String, Iterable<User>>> kvpUserIdGroup = kvpUserIdUser.apply(GroupByKey.create());

			//calculate userStats by gender
			PCollection<KV<String, User>> kvpGenderUserStats = kvpUserIdGroup.apply(ParDo.of(new CalculateUserStats()));

			//group userStats by gender
			PCollection<KV<String, Iterable<User>>> kvpGroup = kvpGenderUserStats.apply(GroupByKey.create());

			//calculate variance and mean for each gender
			return kvpGroup.apply(ParDo.of(new CalculateTTestStats()));
		}
	}
}
