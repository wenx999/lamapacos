/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lamapacos.preprocessor.filter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/** Dump the content of a segment. */
public class SegmentFilter extends Configured implements Reducer<Text, NutchWritable, Text, Text> {

	public static final Logger LOG = LoggerFactory.getLogger(SegmentFilter.class);

	long recNo = 0L;

	private boolean co;//, fe, ge, pa, pd, pt;
	private FileSystem fs;

	public static class InputCompatMapper extends MapReduceBase implements Mapper<WritableComparable, Writable, Text, NutchWritable> {
		private Text newKey = new Text();

		@Override
		public void map(WritableComparable key, Writable value, OutputCollector<Text, NutchWritable> collector, Reporter reporter) throws IOException {
			// convert on the fly from old formats with UTF8 keys.
			// UTF8 deprecated and replaced by Text.
			if (key instanceof Text) {
				newKey.set(key.toString());
				key = newKey;
			}
			collector.collect((Text) key, new NutchWritable(value));
		}

	}

	/** Implements a text output format */
	public static class TextOutputFormat extends FileOutputFormat<WritableComparable, Writable> {
		@Override
		public RecordWriter<WritableComparable, Writable> getRecordWriter(final FileSystem fs, JobConf job, String name, final Progressable progress)
				throws IOException {

			final Path segmentDumpFile = new Path(FileOutputFormat.getOutputPath(job), name);

			// Get the old copy out of the way
			if (fs.exists(segmentDumpFile)) fs.delete(segmentDumpFile, true);

			final PrintStream printStream = new PrintStream(fs.create(segmentDumpFile));
			return new RecordWriter<WritableComparable, Writable>() {
				@Override
				public synchronized void write(WritableComparable key, Writable value) throws IOException {
					printStream.println(value);
				}

				@Override
				public synchronized void close(Reporter reporter) throws IOException {
					printStream.close();
				}
			};
		}
	}

	public SegmentFilter() {
		super(null);
	}

	public SegmentFilter(Configuration conf, boolean co, boolean fe, boolean ge, boolean pa, boolean pd, boolean pt) {
		super(conf);
		this.co = co;
		try {
			this.fs = FileSystem.get(getConf());
		} catch (IOException e) {
			LOG.error("IOException:", e);
		}
	}

	@Override
	public void configure(JobConf job) {
		setConf(job);
		this.co = getConf().getBoolean("segment.reader.co", true);
		try {
			this.fs = FileSystem.get(getConf());
		} catch (IOException e) {
			LOG.error("IOException:", e);
		}
	}

	private JobConf createJobConf() {
		JobConf job = new NutchJob(getConf());
		job.setBoolean("segment.reader.co", this.co);
		return job;
	}

	@Override
	public void close() {
	}

	@Override
	public void reduce(Text key, Iterator<NutchWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		StringBuffer dump = new StringBuffer();

		dump.append("\nRecno:: ").append(recNo++).append("\n");
		dump.append("URL:: " + key.toString() + "\n");
		while (values.hasNext()) {
			Writable value = values.next().get(); // unwrap
			if (value instanceof CrawlDatum) {
				dump.append("\nCrawlDatum::\n").append(((CrawlDatum) value).toString());
			} else if (value instanceof Content) {
				dump.append("\nContent::\n").append(((Content) value).toString());
			} else if (value instanceof ParseData) {
				dump.append("\nParseData::\n").append(((ParseData) value).toString());
			} else if (value instanceof ParseText) {
				dump.append("\nParseText::\n").append(((ParseText) value).toString());
			} else if (LOG.isWarnEnabled()) {
				LOG.warn("Unrecognized type: " + value.getClass());
			}
		}
		output.collect(key, new Text(dump.toString()));
	}

	public void dump(Path segment, Path output) throws IOException {

		if (LOG.isInfoEnabled()) {
			LOG.info("SegmentReader: dump segment: " + segment);
		}

		JobConf job = createJobConf();
		job.setJobName("read " + segment);

		if (co) FileInputFormat.addInputPath(job, new Path(segment, Content.DIR_NAME));

		job.setInputFormat(SequenceFileInputFormat.class);
		job.setMapperClass(InputCompatMapper.class);
		job.setReducerClass(SegmentFilter.class);

		Path tempDir = new Path(job.get("hadoop.tmp.dir", "/tmp") + "/segread-" + new java.util.Random().nextInt());
		fs.delete(tempDir, true);

		FileOutputFormat.setOutputPath(job, tempDir);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NutchWritable.class);

		JobClient.runJob(job);

		// concatenate the output
		Path dumpFile = new Path(output, job.get("segment.dump.dir", "dump"));

		// remove the old file
		fs.delete(dumpFile, true);
		FileStatus[] fstats = fs.listStatus(tempDir, HadoopFSUtil.getPassAllFilter());
		Path[] files = HadoopFSUtil.getPaths(fstats);

		PrintWriter writer = null;
		int currentRecordNumber = 0;
		if (files.length > 0) {
			writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(fs.create(dumpFile))));
			try {
				for (int i = 0; i < files.length; i++) {
					Path partFile = files[i];
					try {
						currentRecordNumber = append(fs, job, partFile, writer, currentRecordNumber);
					} catch (IOException exception) {
						if (LOG.isWarnEnabled()) {
							LOG.warn("Couldn't copy the content of " + partFile.toString() + " into " + dumpFile.toString());
							LOG.warn(exception.getMessage());
						}
					}
				}
			} finally {
				writer.close();
			}
		}
		fs.delete(tempDir, true);
		if (LOG.isInfoEnabled()) {
			LOG.info("SegmentReader: done");
		}
	}

	/** Appends two files and updates the Recno counter */
	private int append(FileSystem fs, Configuration conf, Path src, PrintWriter writer, int currentRecordNumber) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(src)));
		try {
			String line = reader.readLine();
			while (line != null) {
				if (line.startsWith("Recno:: ")) {
					line = "Recno:: " + currentRecordNumber++;
				}
				writer.println(line);
				line = reader.readLine();
			}
			return currentRecordNumber;
		} finally {
			reader.close();
		}
	}

	private static final int MODE_DUMP = 0;

//	private static final int MODE_LIST = 1;

	private static final int MODE_DEFAULT = -0x3ffff;

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			usage();
			return;
		}
		int mode = MODE_DEFAULT;
		if (args[0].equals("-dump")) mode = MODE_DUMP;
//		else if (args[0].equals("-list")) mode = MODE_LIST;
		
		boolean co = true;
		boolean fe = true;
		boolean ge = true;
		boolean pa = true;
		boolean pd = true;
		boolean pt = true;
		// collect general options
		for (int i = 1; i < args.length; i++) {
			if (args[i].equals("-nocontent")) {
				co = false;
				args[i] = null;
			} else if (args[i].equals("-nofetch")) {
				fe = false;
				args[i] = null;
			} else if (args[i].equals("-nogenerate")) {
				ge = false;
				args[i] = null;
			} else if (args[i].equals("-noparse")) {
				pa = false;
				args[i] = null;
			} else if (args[i].equals("-noparsedata")) {
				pd = false;
				args[i] = null;
			} else if (args[i].equals("-noparsetext")) {
				pt = false;
				args[i] = null;
			}
		}
		Configuration conf = NutchConfiguration.create();
		final FileSystem fs = FileSystem.get(conf);
		SegmentFilter segmentReader = new SegmentFilter(conf, co, fe, ge, pa, pd, pt);
		// collect required args
		switch (mode) {
		case MODE_DUMP:
			String input = args[1];
			if (input == null) {
				System.err.println("Missing required argument: <segment_dir>");
				usage();
				return;
			}
			String output = args.length > 2 ? args[2] : null;
			if (output == null) {
				System.err.println("Missing required argument: <output>");
				usage();
				return;
			}
			segmentReader.dump(new Path(input), new Path(output));
			return;
		default:
			System.err.println("Invalid operation: " + args[0]);
			usage();
			return;
		}
	}

	private static void usage() {
		System.err.println("Usage: SegmentReader (-dump ... | -list ... | -get ...) [general options]\n");
		System.err.println("* General options:");
		System.err.println("\t-nocontent\tignore content directory");
		System.err.println("\t-nofetch\tignore crawl_fetch directory");
		System.err.println("\t-nogenerate\tignore crawl_generate directory");
		System.err.println("\t-noparse\tignore crawl_parse directory");
		System.err.println("\t-noparsedata\tignore parse_data directory");
		System.err.println("\t-noparsetext\tignore parse_text directory");
		System.err.println();
		System.err.println("* SegmentReader -dump <segment_dir> <output> [general options]");
		System.err.println("  Dumps content of a <segment_dir> as a text file to <output>.\n");
		System.err.println("\t<segment_dir>\tname of the segment directory.");
		System.err.println("\t<output>\tname of the (non-existent) output directory.");
		System.err.println();
		System.err.println("* SegmentReader -list (<segment_dir1> ... | -dir <segments>) [general options]");
		System.err.println("  List a synopsis of segments in specified directories, or all segments in");
		System.err.println("  a directory <segments>, and print it on System.out\n");
		System.err.println("\t<segment_dir1> ...\tlist of segment directories to process");
		System.err.println("\t-dir <segments>\t\tdirectory that contains multiple segments");
		System.err.println();
		System.err.println("* SegmentReader -get <segment_dir> <keyValue> [general options]");
		System.err.println("  Get a specified record from a segment, and print it on System.out.\n");
		System.err.println("\t<segment_dir>\tname of the segment directory.");
		System.err.println("\t<keyValue>\tvalue of the key (url).");
		System.err.println("\t\tNote: put double-quotes around strings with spaces.");
	}
}
