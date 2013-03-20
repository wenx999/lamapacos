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

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dump the content of a segment. */
public class SegmentFilter extends Configured implements Reducer<Text, NutchWritable, Text, Text> {

	public static final Logger LOG = LoggerFactory.getLogger(SegmentFilter.class);

	private FileSystem fs;
	private static final String URL_REGEX = "directly.accept.url.regex";
	private static final String URL_REGEX_SEP = ";";
	
	public static interface Filter extends Configurable{
		public abstract boolean accept(Object obj);
	}
	
	public static abstract class FilterBase implements Filter {
		Configuration conf;
		public Configuration getConf() {
			return this.conf;
		}
	}
	
	public static  class RegexsFilter extends FilterBase {
		static class PolarPattern {
			char polar;  //‘+’ or '-', which means accept this pattern or not
			Pattern pattern;
			PolarPattern(char polar, Pattern p) {
				this.polar = polar;
				this.pattern = p;
			}
		}
		
		static List<PolarPattern> patterns = new ArrayList<PolarPattern>();
		
		public RegexsFilter() {}
		public static void setPatterns(Configuration conf) {
			String regexs = conf.get(URL_REGEX, "+.*");
			String [] splits = regexs.split(URL_REGEX_SEP);
			for(String regex : splits) {
				try{
					PolarPattern p = new PolarPattern(regex.charAt(0),Pattern.compile(regex.substring(1)));
					patterns.add(p);
				} catch(PatternSyntaxException e) {
					throw new IllegalArgumentException("Invalid pattern: "+regex);
				}
			}
		}
		@Override
		public boolean accept(Object obj) {
			for(PolarPattern p : patterns) {
				if(p.polar == '-' && p.pattern.matcher(obj.toString()).matches()) {
					return false;
				} else if(p.polar == '+' && p.pattern.matcher(obj.toString()).matches()) {
					return true;
				}
			}
			return false;
		}

		@Override
		public void setConf(Configuration conf) {
			this.conf = conf;
		}
		
	}
	
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

	public SegmentFilter(Configuration conf) {
		super(conf);
		conf.addResource("lamapacos-preprocessor.xml");
		RegexsFilter.setPatterns(conf);
		try {
			this.fs = FileSystem.get(getConf());
		} catch (IOException e) {
			LOG.error("IOException:", e);
		}
	}

	private RegexsFilter regexsFilter = new RegexsFilter();
	@Override
	public void configure(JobConf job) {
		setConf(job);
		//conf file
		job.addResource("lamapacos-preprocessor.xml");
		RegexsFilter.setPatterns(job);
		try {
			this.fs = FileSystem.get(getConf());
		} catch (IOException e) {
			LOG.error("IOException:", e);
		}
	}

	private JobConf createJobConf() {
		JobConf job = new NutchJob(getConf());
		return job;
	}

	@Override
	public void close() {
	}

	@Override
	public void reduce(Text key, Iterator<NutchWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		StringBuffer dump = new StringBuffer();
		if(!regexsFilter.accept(key.toString())) return;
		
		while (values.hasNext()) {
			Writable value = values.next().get(); // unwrap	
			if (value instanceof Content) {
				dump.append(((Content) value).toString());
			} else if (LOG.isWarnEnabled()) {
				LOG.warn("Unrecognized type: " + value.getClass());
			}
		}
		output.collect(key, new Text(dump.toString()));
	}

	public void dump(Path segmentHome, Path output) throws IOException {

		if (LOG.isInfoEnabled()) {
			LOG.info("SegmentFilter: filtering segment: " + segmentHome);
		}

		JobConf job = createJobConf();
		job.setJobName("filter " + segmentHome);
		
		FileStatus[] paths = fs.listStatus(segmentHome);
		for(FileStatus path : paths) {
			if(path.isDir()) {
				FileInputFormat.addInputPath(job, new Path(segmentHome,new Path(path.getPath(), Content.DIR_NAME)));
			}
		}
		
		
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setMapperClass(InputCompatMapper.class);
		job.setReducerClass(SegmentFilter.class);

//		Path tempDir = new Path(job.get("hadoop.tmp.dir", "/tmp") + "/segfilter-" + new java.util.Random().nextInt());
//		fs.delete(tempDir, true);

		FileOutputFormat.setOutputPath(job, output);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NutchWritable.class);

		JobClient.runJob(job);

		if (LOG.isInfoEnabled()) {
			LOG.info("SegmentFilter: done");
		}
	}


	private static final int MODE_DUMP = 0;
	private static final int MODE_DEFAULT = -0x3ffff;

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			usage();
			return;
		}
		int mode = MODE_DEFAULT;
		if (args[0].equals("-dump")) mode = MODE_DUMP;
		else {
			System.err.println("Missing operation argument:(-dump... | ...)");
			usage();
			return;
		}
		
		Configuration conf = NutchConfiguration.create();
		final FileSystem fs = FileSystem.get(conf);
		SegmentFilter segmentReader = new SegmentFilter(conf);
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
		System.err.println("Usage: SegmentFilter (-dump ... | ...) [general options]\n");
		System.err.println("* General options:");
		System.err.println();
		System.err.println("* SegmentReader -dump <segment_dir> <output> [general options]");
		System.err.println("  Dumps content of a <segment_dir> as a text file to <output>.\n");
		System.err.println("\t<segment_dir>\tname of the segment home.");
		System.err.println("\t<output>\tname of the (non-existent) output directory.");
		System.err.println();
		System.err.println("\t\tNote: put double-quotes around strings with spaces.");
	}
}
