package com.romelj.dataflow.transforms;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringDelegateCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PInput;

import java.io.File;
import java.net.URI;

/**
 * Reads the documents at the provided uris and returns all lines
 * from the documents.
 */
public class ReadDocuments extends PTransform<PInput, PCollection<String>> {
	private final Iterable<URI> uris;

	public ReadDocuments(Iterable<URI> uris) {
		this.uris = uris;
	}

	@Override
	public Coder<?> getDefaultOutputCoder() {
		return KvCoder.of(StringDelegateCoder.of(URI.class), StringUtf8Coder.of());
	}

	@Override
	public PCollection<String> apply(PInput input) {
		Pipeline pipeline = input.getPipeline();

		// Create one TextIO.Read transform for each document
		// and add its output to a PCollectionList
		PCollectionList<String> urisToLines = PCollectionList.empty(pipeline);

		// TextIO.Read supports:
		//  - file: URIs and paths locally
		//  - gs: URIs on the service
		for (final URI uri : uris) {
			String uriString;
			if (uri.getScheme().equals("file")) {
				uriString = new File(uri).getPath();
			} else {
				uriString = uri.toString();
			}

			PCollection<String> oneUriToLines = pipeline.apply(TextIO.Read.from(uriString).named("TextIO.Read(" + uriString + ")"));
			urisToLines = urisToLines.and(oneUriToLines);
		}

		return urisToLines.apply(Flatten.pCollections());
	}
}