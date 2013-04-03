/**
 * 
 */
package org.lamapacos.preprocessor.extraction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hadoop
 *
 */
public class Splitter extends Configured{
	public static final Logger LOG = LoggerFactory.getLogger(Splitter.class);
	Splitter() {}
	public Splitter(Configuration conf) {
		setConf(conf);
	}
	

	
	public static class SplitPhaseMapper extends Mapper<WritableComparable, LamapacosArrayWritable, WritableComparable, Writable> {
		private Tokenizer tokenizer = new NLPIRTokenizer();
		@Override
		public void map(WritableComparable key, LamapacosArrayWritable value, Context context) 
			throws IOException, InterruptedException{
			Writable[] records = value.get();
			for(Writable wrappedRecord : records) {
				Writable record = ((LamapacosWritable) wrappedRecord).get();
				if(record instanceof ScoredContent) {
					ScoredContent content = (ScoredContent)record;
					String outputVal = tokenizer.segment(content.getContent());
					context.write(key, new Text(outputVal));
				} else {
					String outputVal = tokenizer.segment(record.toString());
					context.write(key, new Text(outputVal));
				}
			}
		}
	}
	
	public void split(Path[] sourceHome, Path output ) throws IOException, ClassNotFoundException, InterruptedException {
		
		if(LOG.isInfoEnabled()) {
			Log.info("SplitPhase: splitting source: " + sourceHome);
		}
		Configuration conf = getConf() == null ? new Configuration() : getConf();
		Job job = new Job(conf, "split " + sourceHome);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, sourceHome);
		FileOutputFormat.setOutputPath(job, output);
		
		job.setMapperClass(SplitPhaseMapper.class);
		job.setNumReduceTasks(0);
		job.setSpeculativeExecution(false);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LamapacosWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		int ret = job.waitForCompletion(true) ? 0 : 1;
		
		if(LOG.isInfoEnabled()) {
			LOG.info("SplitPhase: done");
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
		
		if(!args[0].equals("-split")) {
			System.err.println("Missing operation argument:(-split ...)");
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
		new Splitter(new Configuration()).split(list.toArray(new Path[list.size()]), new Path(output));
	}
	
	
	public static void usage() {
		System.err.println("Usage: SplitPhase (-split ... ) [general options]\n");
		System.err.println("* General options:");
		System.err.println();
		System.err.println("* Splitter -split <source_dir> <output> [-dir dir1 -dir dir2...]");
		System.err.println(" split content of a <source_dir> to <output> from pages into tokenized-Content.\n");
		System.err.println("\t<source_dir>\tname of the souce home.");
		System.err.println("\t<output>\tname of the (non-existent) output directory.");
		System.err.println();
	}
}
