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
import com.romelj.dataflow.entities.User;
import com.romelj.dataflow.entities.json.UsageGenderJson;
import com.romelj.dataflow.utils.Options;
import com.romelj.dataflow.utils.StringOp;

import java.io.IOException;
import java.net.URISyntaxException;

import static com.romelj.dataflow.utils.FileOp.listInputDocuments;

/**
 * Which gender watches more movies, males of females?
 *
 */
public class Task3 {

	/**
	 * Run movies Task 3
	 *
	 * @param input - local or gcs input path
	 * @param output - local or gcs output path
	 * @param locally - run task locally or on cloud data flow
	 * @throws IOException - exception at reading files
	 * @throws URISyntaxException - exception at reading files
	 */
	public static void runTask(String input, String output, boolean locally) throws IOException, URISyntaxException {
		if (!Strings.isNullOrEmpty(input) && !Strings.isNullOrEmpty(output)) {
			Options options = PipelineOptionsFactory.as(com.romelj.dataflow.utils.Options.class);
			options.setJobName(Task3.class.getClass().getSimpleName());
			options.setProject(Settings.projectId);
			options.setInput(input);
			options.setOutput(output);

			if (!locally) {
				//run task on cloud data flow and set runner and staging location
				options.setRunner(BlockingDataflowPipelineRunner.class);
				options.setStagingLocation(Settings.stagingLocation);
			}

			Pipeline p = Pipeline.create(options);

			// pipeline applies the composite MoviesWatchedByGender transform, and passes the static FormatAsTextFn() to the ParDo transform.
			p.apply(new ReadDocuments(listInputDocuments(options)))
					.apply("MoviesWatchedByGender", new MoviesWatchedByGender())
					.apply("FormatAsText", MapElements.via(new Task3.FormatAsTextFn()))
					.apply(TextIO.Write.named("WriteMoviesWatchedByGender").to(options.getOutput()));
			p.run();
		}
	}

	/**
	 * DoFn counts number of movies watched for each user and returns KV<gender, numMovies>
	 */
	private static class CountMoviesWatched extends DoFn<KV<String, Iterable<User>>, KV<String, Integer>> {

		@Override
		public void processElement(ProcessContext c) throws Exception {
			String gender = StringOp.getGender(c.element().getValue());
			int movies = 0;
			for (User ignored : c.element().getValue()) {
				movies++;
			}
			c.output(KV.of(gender, movies));
		}
	}

	/**
	 * A PTransform that converts a PCollection containing lines of text into a PCollection of
	 * mean of movies watched by gender
	 */
	private static class MoviesWatchedByGender extends PTransform<PCollection<String>, PCollection<KV<String, Double>>> {

		@Override
		public PCollection<KV<String, Double>> apply(PCollection<String> lines) {

			//Read the data as KV pairs, where userId is the key and User object is the value. We actually need only gender.
			PCollection<KV<String, User>> kvpUserIdUser = lines.apply(ParDo.of(new ExtractMoviesData()));

			//group User objects by userId
			PCollection<KV<String, Iterable<User>>> groupUsers = kvpUserIdUser.apply(GroupByKey.create());

			//count the number of movies watched for each gender
			PCollection<KV<String, Integer>> userGender = groupUsers.apply(ParDo.of(new CountMoviesWatched()));

			//calculate mean for each gender
			return userGender.apply(Mean.perKey());

		}
	}

	/**
	 * A SimpleFunction that converts mean of movies by gender to json.
	 */
	private static class FormatAsTextFn extends SimpleFunction<KV<String, Double>, String> {

		@Override
		public String apply(KV<String, Double> input) {
			Gson gson = new Gson();
			UsageGenderJson usageGenderJson = new UsageGenderJson(input.getKey(), input.getValue());
			return gson.toJson(usageGenderJson);
		}
	}
}
