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
import com.romelj.dataflow.entities.json.MoviesByUsersJson;
import com.romelj.dataflow.utils.Options;

import java.io.IOException;
import java.net.URISyntaxException;

import static com.romelj.dataflow.utils.FileOp.listInputDocuments;

/**
 * How many movies does each user rate?
 *
 *
 */
public class Task1 {

	/**
	 * Run movies Task 1
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
			options.setJobName(Task1.class.getClass().getSimpleName());
			options.setProject(Settings.projectId);
			options.setInput(input);
			options.setOutput(output);


			if (!locally) {
				//run task on cloud data flow and set runner and staging location
				options.setRunner(BlockingDataflowPipelineRunner.class);
				options.setStagingLocation(Settings.stagingLocation);
			}

			Pipeline p = Pipeline.create(options);

			// pipeline applies the composite MeanMoviesByUsers transform, and passes the static FormatAsTextFn() to the ParDo transform.
			p.apply(new ReadDocuments(listInputDocuments(options)))
					.apply("MeanMoviesByUsers", new MeanMoviesByUsers())
					.apply("FormatAsText", MapElements.via(new FormatAsTextFn()))
					.apply(TextIO.Write.named("WriteMeanScore").to(options.getOutput()));
			p.run();
		}
	}

	/**
	 * A SimpleFunction that converts mean of movies watched by users to json.
	 */
	private static class FormatAsTextFn extends SimpleFunction<Double, String> {
		@Override
		public String apply(Double score) {
			Gson gson = new Gson();
			MoviesByUsersJson moviesByUsersJson = new MoviesByUsersJson(score);
			return gson.toJson(moviesByUsersJson, MoviesByUsersJson.class);
		}
	}

	/**
	 * A PTransform that converts a PCollection containing lines of text into a PCollection of
	 * mean of movies watched by users
	 */
	private static class MeanMoviesByUsers extends PTransform<PCollection<String>, PCollection<Double>> {

		@Override
		public PCollection<Double> apply(PCollection<String> lines) {

			//Read the data as KV pairs, where userId is the key and User object is the value. We actually need only String movieId.
			PCollection<KV<String, User>> kvpUserIdUser = lines.apply(ParDo.of(new ExtractMoviesData()));

			//Count by key the number of User Objects (movies) for each userId. We assume that there is not two equal movieIds per userId.
			PCollection<KV<String, Long>> countPerKey = kvpUserIdUser.apply(Count.perKey());

			//Calculate the mean of movies watched by users
			return countPerKey.apply(Values.create()).apply(Mean.globally());
		}
	}

}
