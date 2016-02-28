package com.romelj.dataflow.utils;

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Description;

/**
 *
 * Inherits standard configuration options.
 */
public interface Options extends DataflowPipelineOptions {
	@Description("Path of the folder to read from")
	String getInput();

	void setInput(String value);

	@Description("Path of the folder to write to")
	String getOutput();

	void setOutput(String value);

}