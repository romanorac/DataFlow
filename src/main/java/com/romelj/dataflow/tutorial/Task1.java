package com.romelj.dataflow.tutorial;

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
import com.romelj.dataflow.dofn.ExtractTutorialData;
import com.romelj.dataflow.transforms.ReadDocuments;
import com.romelj.dataflow.entities.json.DurationCountJson;
import com.romelj.dataflow.utils.Options;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import static com.romelj.dataflow.utils.FileOp.listInputDocuments;

/**
 * optimise the duration of the tutorial button
 * find the best amount of time for a button to stay on the screen
 */
public class Task1 {

	/**
	 * Run tutorial Task 1
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

			// pipeline applies the composite ProcessTimestamp transform, and passes the static FormatAsTextFn() to the ParDo transform.
			p.apply(new ReadDocuments(listInputDocuments(options)))
					.apply("ProcessTimestamp", new ProcessTimestamp())
					.apply("FormatAsText", MapElements.via(new FormatAsTextFn()))
					.apply(TextIO.Write.named("WriteDurations").to(options.getOutput()));
			p.run();
		}
	}

	/**
	 * DoFn calculates duration between app install and watch tutorial. It outputs duration rounded to nearest minute.
	 */
	private static class CalculateDurations extends DoFn<KV<String, Iterable<Long>>, Long> {

		private final static long minutesYear = 525600L; //number of minutes in one year. We do not output data that is older
		private final Aggregator<Long, Long> incorrectData = createAggregator("incorrectData", new Sum.SumLongFn());

		@Override
		public void processElement(ProcessContext c) throws Exception {
			int count = 0;
			long timestamp1 = 0L;
			long timestamp2 = 0L;
			for (Long timestamp : c.element().getValue()) {
				if (count == 0) {
					timestamp1 = timestamp; //can be install or watched tutorial timestamp
				} else if (count == 1) {
					timestamp2 = timestamp; //can be install or watched tutorial timestamp
				} else {
					incorrectData.addValue(1L); //error event
				}
				count++;
			}


			if (count == 2) { //there should be exactly two events

				if (timestamp2 != 0L) {
					//we assume that install_timestamp is always smaller than tutorial_timestamp (no corrupt data)
					long duration = Math.abs(timestamp1 - timestamp2);
					long minutes = TimeUnit.MILLISECONDS.toMinutes(duration); //round millis to minutes

					if (minutes < minutesYear) { // duration should be smaller than one year
						c.output(minutes);
					}

				}
			}
		}
	}

	/**
	 * A SimpleFunction that converts tutorial views grouped by duration to json.
	 */
	private static class FormatAsTextFn extends SimpleFunction<KV<Long, Long>, String> {

		@Override
		public String apply(KV<Long, Long> kvp) {
			Gson gson = new Gson();
			DurationCountJson durationCountJson = new DurationCountJson(kvp.getKey(), kvp.getValue());
			return gson.toJson(durationCountJson, DurationCountJson.class);
		}
	}

	/**
	 * A PTransform that converts a PCollection containing lines of text into a PCollection of
	 * KV<durationInMinutes, numberOfDurations>
	 */
	private static class ProcessTimestamp extends PTransform<PCollection<String>, PCollection<KV<Long, Long>>> {

		@Override
		public PCollection<KV<Long, Long>> apply(PCollection<String> lines) {

			// Read the data
			PCollection<KV<String, Long>> kvpUserIdTimestamp = lines.apply(ParDo.of(new ExtractTutorialData()));

			//group timestamps by userId
			PCollection<KV<String, Iterable<Long>>> groupTimestamp = kvpUserIdTimestamp.apply(GroupByKey.create());

			//calculate duration in minutes (between app install and watches tutorial) for each user
			PCollection<Long> durations = groupTimestamp.apply(ParDo.of(new CalculateDurations()));

			//count number of equal durations
			return durations.apply(Count.perElement());

		}
	}


}
