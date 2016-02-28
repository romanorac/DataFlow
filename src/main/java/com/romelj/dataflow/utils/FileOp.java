package com.romelj.dataflow.utils;

import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.gson.Gson;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FileOp {

	/**
	 * Reads all files in a folder and outputs it as a list of objects
	 *
	 * @param folderPath - folder to read
	 * @param clazz - class to map string to object
	 * @param <T> -
	 * @return - List of Class<T> objects
	 * @throws IOException
	 */
	public static <T> List<T> readFiles(String folderPath, Class<T> clazz) throws IOException {
		final List<String> filePaths = new ArrayList<>();
		Files.walk(Paths.get(folderPath)).forEach(filePath -> {
			if (Files.isRegularFile(filePath)) {
				filePaths.add(filePath.toAbsolutePath().toString());
			}
		});

		List<T> objectsList = new ArrayList<>();
		for (String filePath : filePaths) {
			objectsList.addAll(readFile(filePath, clazz));
		}
		return objectsList;
	}

	/**
	 * Reads a file a outputs it as a list of objects
	 *
	 * @param filename - filename to read
	 * @param clazz - class to map string to object
	 * @param <T> -
	 * @return - List of Class<T> objects
	 */
	private static <T> List<T> readFile(String filename, Class<T> clazz) {
		List<T> objectList = new ArrayList<>();
		String line;

		Gson gson = new Gson();

		try {
			// FileReader reads text files in the default encoding.
			FileReader fileReader = new FileReader(filename);

			// Always wrap FileReader in BufferedReader.
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			while ((line = bufferedReader.readLine()) != null) {
				T object = gson.fromJson(line, clazz);
				objectList.add(object);
			}

			// close files.
			bufferedReader.close();
		} catch (FileNotFoundException ex) {
			System.out.println("Unable to open file '" + filename + "'");
		} catch (IOException ex) {
			System.out.println("Error reading file '" + filename + "'");
		}

		return objectList;
	}


	/**
	 * Lists documents contained beneath the {@code options.input} prefix/directory.
	 */
	public static Set<URI> listInputDocuments(Options options) throws URISyntaxException, IOException {
		URI baseUri = new URI(options.getInput());

		// List all documents in the directory or GCS prefix.
		URI absoluteUri;
		if (baseUri.getScheme() != null) {
			absoluteUri = baseUri;
		} else {
			absoluteUri = new URI(
					"file",
					baseUri.getAuthority(),
					baseUri.getPath(),
					baseUri.getQuery(),
					baseUri.getFragment());
		}

		Set<URI> uris = new HashSet<>();
		if (absoluteUri.getScheme().equals("file")) {
			File directory = new File(absoluteUri);
			for (String entry : directory.list()) {
				File path = new File(directory, entry);
				uris.add(path.toURI());
			}
		} else if (absoluteUri.getScheme().equals("gs")) {
			GcsUtil gcsUtil = options.as(GcsOptions.class).getGcsUtil();
			URI gcsUriGlob = new URI(
					absoluteUri.getScheme(),
					absoluteUri.getAuthority(),
					absoluteUri.getPath() + "*",
					absoluteUri.getQuery(),
					absoluteUri.getFragment());
			for (GcsPath entry : gcsUtil.expand(GcsPath.fromUri(gcsUriGlob))) {
				uris.add(entry.toUri());
			}
		}

		return uris;
	}
}
