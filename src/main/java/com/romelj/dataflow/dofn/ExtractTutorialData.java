package com.romelj.dataflow.dofn;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;
import com.romelj.dataflow.Settings;
import com.romelj.dataflow.utils.FilterOp;

/**
 * This DoFn tokenizes lines of text into TutorialEvent objects; we pass it to a ParDo in the
 * pipeline.
 */
public class ExtractTutorialData extends DoFn<String, KV<String, Long>> {

	private final Aggregator<Long, Long> incorrectData = createAggregator("incorrectData", new Sum.SumLongFn());

	@Override
	public void processElement(ProcessContext c) {
		if (c.element().trim().isEmpty() || c.element().startsWith("#")) {
			incorrectData.addValue(1L);
		} else {
			// Split the line into words.
			String[] entity = c.element().split(Settings.delimiter);
			if (FilterOp.filterTutorialEvents(entity)) { //it also skips header
				String userId = entity[0];
				long timestamp = Long.parseLong(entity[1].trim());

				c.output(KV.of(userId, timestamp));

			} else {
				incorrectData.addValue(1L);
			}
		}

	}
}
