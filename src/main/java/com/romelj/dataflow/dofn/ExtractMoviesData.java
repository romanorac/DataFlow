package com.romelj.dataflow.dofn;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;
import com.romelj.dataflow.Settings;
import com.romelj.dataflow.entities.User;
import com.romelj.dataflow.entities.UserEvent;
import com.romelj.dataflow.utils.FilterOp;

/**
 * DoFn is used in all movie task to read the data. This presents some overhead as some tasks does not require all data.
 *
 * This DoFn tokenizes lines of text into UserEvent objects; we pass it to a ParDo in the
 * pipeline.
 *
 */
public class ExtractMoviesData extends DoFn<String, KV<String, User>> {
	private final Aggregator<Long, Long> incorrectData = createAggregator("incorrectData", new Sum.SumLongFn());

	@Override
	public void processElement(ProcessContext c) {
		if (c.element().trim().isEmpty() || c.element().startsWith("#")) {
			incorrectData.addValue(1L); //count lines that cannot be parsed

		} else {
			// Split the line into words.
			String[] entity = c.element().split(Settings.delimiter);
			if (FilterOp.filterMovieEvents(entity)) { //check each line. It also skips header
				// Output each UserEvent encountered into the output PCollection.
				String userId = entity[0];
				String gender = entity[1];
				String movieId = entity[2];
				int score = Integer.parseInt(entity[3]);

				User userEvent = new UserEvent(userId, gender, movieId, score);
				c.output(KV.of(userId, userEvent));

			} else {
				incorrectData.addValue(1L); //count lines that cannot be parsed
			}
		}

	}

}