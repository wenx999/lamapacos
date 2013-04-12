package org.lamapacos.trainingset;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.lamapacos.io.LamapacosWritable;
import org.lamapacos.io.TaggedContent;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SumOfWeightTagger should extends DictBasedTagger.
 * @author hadoop
 *
 */
@SuppressWarnings("rawtypes")
public class SumOfWeightTagger extends Mapper<WritableComparable, LamapacosWritable, WritableComparable, Writable> {
	public static final Logger LOG = LoggerFactory.getLogger(SumOfWeightTagger.class);
	public static final String WORD_SEP = " ";
	public static final char WORD_CHARACTERISTIC = '/';
	public static final int DEGREE_SCOPE = 8;
	public static final String PUNCTUATION_MARK_LABEL = "w";
	public static final String DEFAULT_SENTIMENT = "CO";
	
	private Configuration conf;
	
	public SumOfWeightTagger() {
	}
	
	public SumOfWeightTagger(Configuration conf) {
		setConf(conf);
	}
	
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
	
	public Configuration getConf() {
		return this.conf;
	}
	

	private static SimpleTagger simpleTag;
	@Override
	public void setup(Context context) {
		simpleTag = new SimpleTagger(context.getConfiguration());
	}
	
	/**
	 * 
	 * @param key
	 * @param value
	 * @param context
	 */
	@Override
	public void map(WritableComparable key, LamapacosWritable value, Context context) 
		throws IOException, InterruptedException {
		TaggedContent taggedContent = simpleTag.tag(value);
		context.write(key, taggedContent);
	}
	
	/**
	 * tag phase, tag all K-V pair
	 * @param sourceHome
	 * @param output
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public void tagAll(Path[] sourceHome, Path output) throws IOException, InterruptedException, ClassNotFoundException {
		if(LOG.isInfoEnabled()) {
			Log.info("TagPhase: taging source:" + sourceHome);
		}
		
		Configuration conf = getConf() == null ? new Configuration():getConf();
		Job job = new Job(conf, "tag " + sourceHome);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, sourceHome);
		FileOutputFormat.setOutputPath(job, output);
		job.setMapperClass(SumOfWeightTagger.class);
		job.setNumReduceTasks(0);
		job.setSpeculativeExecution(false);
		job.setMapOutputKeyClass(Writable.class);
		job.setMapOutputValueClass(TaggedContent.class);
		int ret = job.waitForCompletion(true) ? 0:1;
		if(LOG.isInfoEnabled()) {
			LOG.info("TagPhase: done");
		}
		System.exit(ret);
	}
	
	public void tagAll(Path sourceHome, Path output) throws IOException, InterruptedException, ClassNotFoundException {
		if(LOG.isInfoEnabled()) {
			Log.info("TagPhase: taging source:" + sourceHome);
		}
		
		Configuration conf = getConf() == null ? new Configuration():getConf();
		Job job = new Job(conf, "tag " + sourceHome);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, sourceHome);
		FileOutputFormat.setOutputPath(job, output);
		job.setMapperClass(SumOfWeightTagger.class);
		job.setNumReduceTasks(0);
		job.setSpeculativeExecution(false);
		job.setMapOutputKeyClass(Writable.class);
		job.setMapOutputValueClass(TaggedContent.class);
		int ret = job.waitForCompletion(true) ? 0:1;
		if(LOG.isInfoEnabled()) {
			LOG.info("TagPhase: done");
		}
		System.exit(ret);
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length < 2) {
			usage();
			return;
		}
		
		if(!args[0].equals("-tag")) {
			System.err.println("Missing operation argument:(-tag ...)");
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
		new SumOfWeightTagger(new Configuration()).tagAll(list.toArray(new Path[list.size()]), new Path(output));
	}
	
	public static void usage() {
		System.err.println("Usage: TagPhase (-tag ... ) [general options]\n");
		System.err.println("* General options:");
		System.err.println();
		System.err.println("* TagPhase -tag <source_dir> <output> [-dir dir1 -dir dir2...]");
		System.err.println("  tag content of a <source_dir> to <output>.");
		System.err.println("  Each output contains the original content and its tag, which indicate the opinion.\n");
		System.err.println("\t<source_dir>\tname of the souce home.");
		System.err.println("\t<output>\tname of the (non-existent) output directory.");
		System.err.println();
	}
}