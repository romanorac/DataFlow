package com.romelj.dataflow.movies;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.gson.Gson;
import com.romelj.dataflow.Settings;
import com.romelj.dataflow.dofn.ExtractMoviesData;
import com.romelj.dataflow.transforms.ReadDocuments;
import com.romelj.dataflow.entities.User;
import com.romelj.dataflow.entities.json.UsageGenderJson;
import com.romelj.dataflow.utils.StringOp;

import java.io.IOException;
import java.net.URISyntaxException;

import static com.romelj.dataflow.utils.FileOp.listInputDocuments;

/**
 * Is the app used more by females or males?
 *
 */
public class Task2 {

	/**
	 * Run movies Task 2
	 *
	 * @param input - local or gcs input path
	 * @param output - local or gcs output path
	 * @param locally - run task locally or on cloud data flow
	 * @throws IOException - exception at reading files
	 * @throws URISyntaxException - exception at reading files
	 */
	public static void runTask(String input, String output, boolean locally) throws IOException, URISyntaxException {
		if (!Strings.isNullOrEmpty(input) && !Strings.isNullOrEmpty(output)) {
			com.romelj.dataflow.utils.Options options = PipelineOptionsFactory.as(com.romelj.dataflow.utils.Options.class);
			options.setJobName(Task2.class.getClass().getSimpleName());
			options.setProject(Settings.projectId);
			options.setInput(input);
			options.setOutput(output);

			if (!locally) {
				//run task on cloud data flow and set runner and staging location
				options.setRunner(BlockingDataflowPipelineRunner.class);
				options.setStagingLocation(Settings.stagingLocation);
			}

			Pipeline p = Pipeline.create(options);

			// pipeline applies the composite AppUsageByGender transform, and passes the static FormatAsTextFn() to the ParDo transform.
			p.apply(new ReadDocuments(listInputDocuments(options)))
					.apply("AppUsageByGender", new AppUsageByGender())
					.apply("FormatAsText", MapElements.via(new FormatAsTextFn()))
					.apply(TextIO.Write.named("WriteAppUsage").to(options.getOutput()));
			p.run();
		}
	}

	/**
	 * A SimpleFunction that converts app usage by gender to json.
	 */
	private static class FormatAsTextFn extends SimpleFunction<KV<String, Double>, String> {

		@Override
		public String apply(KV<String, Double> input) {
			Gson gson = new Gson();
			UsageGenderJson usageGenderJson = new UsageGenderJson(input.getKey(), input.getValue());
			return gson.toJson(usageGenderJson);
		}
	}

	/**
	 * DoFn outputs single gender for each user
	 */
	private static class TakeGender extends DoFn<KV<String, Iterable<User>>, String> {

		@Override
		public void processElement(ProcessContext c) throws Exception {
			String gender = StringOp.getGender(c.element().getValue());
			c.output(gender);
		}
	}

	/**
	 * DoFn takes a count by gender, sum of all gender and it calculates the mean
	 */
	private static class MeanGender extends DoFn<KV<String, Long>, KV<String, Double>> {

		private final PCollectionView<Long> sumView;

		MeanGender(PCollectionView<Long> sumView) {
			this.sumView = sumView;
		}

		@Override
		public void processElement(ProcessContext c) throws Exception {
			//calculate the mean for each gender
			String gender = c.element().getKey();
			double ptc = 0;

			Long sum = c.sideInput(sumView);

			if (sum > 0) {
				ptc = (double) c.element().getValue() / sum;
			}

			c.output(KV.of(gender, ptc));
		}
	}

	/**
	 * A PTransform that converts a PCollection containing lines of text into a PCollection of
	 * KV pairs, where gender is the key and percentage is the value
	 */
	private static class AppUsageByGender extends PTransform<PCollection<String>, PCollection<KV<String, Double>>> {

		@Override
		public PCollection<KV<String, Double>> apply(PCollection<String> lines) {

			//Read the data as KV pairs, where userId is the key and User object is the value. We actually need only gender.
			PCollection<KV<String, User>> kvpUserIdUser = lines.apply(ParDo.of(new ExtractMoviesData()));

			//group User objects by userId. We could also used removeDuplicates method, but the KV should be KV<userId, gender>
			PCollection<KV<String, Iterable<User>>> groupUsers = kvpUserIdUser.apply(GroupByKey.create());

			//transform PCollection to contain only genders
			PCollection<String> genders = groupUsers.apply(ParDo.of(new TakeGender()));

			//Count genders
			PCollection<KV<String, Long>> gendersCount = genders.apply(Count.perElement());

			//Number of all genders to calculate the percentage by gender
			PCollectionView<Long> sumView = gendersCount.apply(Values.create()).apply(Sum.longsGlobally()).apply(View.asSingleton());

			//a PCollection of KV pairs, where gender is the key and mean is the value
			return gendersCount.apply(ParDo.withSideInputs(sumView).of(new MeanGender(sumView)));
		}
	}
}
