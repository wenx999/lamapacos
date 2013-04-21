package org.lamapacos.trainingset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import org.lamapacos.io.MultipleTextOutputFormat;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GenerateTrainSet.java TODO some description
 * @author root
 * @version 1.0    2012-8-22
 * 
 */
public class GenerateTrainSet {
	public static final Logger LOG = LoggerFactory.getLogger(GenerateTrainSet.class);
	GenerateTrainSet() {}
	public static class TrainSetMapper extends MapReduceBase implements Mapper<Writable, Text, Text, Text> {
		Map<String, Integer> counterMap = new HashMap<String, Integer>();
		@Override
		public void map(Writable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			float v = (float) (getRandom(0, 100) /100.0);
			Text outputKey = new Text();
			int begin = value.toString().lastIndexOf(":")-2;
			String tag = value.toString().substring(begin, begin+2);
			boolean flag = false;
			if(!counterMap.containsKey(tag)) {
				counterMap.put(tag, 0);
				output.collect(new Text("TrainSet"), value);
				flag = true;
			} else {
				int sum = counterMap.get(tag);
				counterMap.put(tag, sum+1);
			}
			if(v > 0.18 && !flag) {
				outputKey.set("TrainSet");
			} else {
				outputKey.set("TestSet");
			}
			output.collect(outputKey, value);
		}
	}
	public static class TrainSetReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			while(values.hasNext()) {
				output.collect(key, values.next());
			}
		}
		
	}
	public static float getRandom(int start, int end) {
		if(start > end || start < 0 || end < 0) {
			return -1;
		}
		return (float)(Math.random()*(end - start +1)) + start;
	}
	
	/**
	 * training the data
	 * @param sourceHome
	 * @param output
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public void train(Path[] sourceHome, Path output) throws IOException, InterruptedException, ClassNotFoundException {
		if(LOG.isInfoEnabled()) {
			Log.info("TrainSet: trainset source:" + sourceHome);
		}
		
		JobConf job = new JobConf(GenerateTrainSet.class);
		job.setJobName("GenerateTrainSet");
		job.setJarByClass(GenerateTrainSet.class);
		job.setInputFormat(TextInputFormat.class);
		job.setMapperClass(TrainSetMapper.class);
		job.setCombinerClass(TrainSetReduce.class);
		job.setReducerClass(TrainSetReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormat(MultipleTextOutputFormat.class);
		FileInputFormat.setInputPaths(job, sourceHome);
		FileOutputFormat.setOutputPath(job, output);
		JobClient.runJob(job);
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if(args.length < 2) {
			usage();
			return;
		}
		
		if(!args[0].equals("-train")) {
			System.err.println("Missing operation argument:(-train ...)");
			usage();
			return;
		}
		
		String input = args[1];
		if(null == input) {
			System.err.println("Missing required argument: <source_dir>");
			usage();
			return;
		}
		String output = args.length > 2 ? args[2] : null;
		if(null == output) {
			System.err.println("Missing required argument: <output>");
			usage();
			return;
		}
		List<Path> list = new ArrayList<Path>();
		list.add(new Path(input));
		if(args.length > 3) {
			if(args.length%2 == 1) {
				for(int i = 2; i < args.length; i++) {
					if(args[i].equals("-dir")) {
						list.add(new Path(args[i++]));
					}
				}
			}else {
				usage();
				return;
			}
		}
		new GenerateTrainSet().train(list.toArray(new Path[list.size()]), new Path(output));
	}
	public static void usage() {
		System.err.println("Usage: TrainingSet (-train ... ) [general options]\n");
		System.err.println("* General options:");
		System.err.println();
		System.err.println("* TrainingSet -train <source_dir> <output> [-dir dir1 -dir dir2...]");
		System.err.println("  tag content of a <source_dir> to <output>.");
		System.err.println("  Each output contains the original content and its tag, which indicate the opinion.\n");
		System.err.println("\t<source_dir>\tname of the souce home.");
		System.err.println("\t<output>\tname of the (non-existent) output directory.");
		System.err.println();
	}
}
