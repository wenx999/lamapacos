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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.lamapacos.io.LamapacosArrayWritable;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hadoop
 *
 */
public class ExtractPhase extends Configured{
	public static final Logger LOG = LoggerFactory.getLogger(ExtractPhase.class);
	ExtractPhase() {}
	public ExtractPhase(Configuration conf) {
		setConf(conf);
	}
	
	public static class ExtractPhaseMapper 
		extends Mapper<WritableComparable, Writable, Writable, Writable> {
		private Text newKey = new Text();
		Extractor extractor = new HtmlExtractor(); //configurable
		@Override
		public void map(WritableComparable key, Writable value, Context context)
			throws IOException, InterruptedException {
			// convert on the fly from old formats with UTF8 keys.
			// UTF8 deprecated and replaced by Text.
//			System.out.println();
			if(key instanceof Text) {
				newKey.set(key.toString());
				key = newKey;
			}
			Writable v = extractor.extract(value);
			if(v.toString().equals("")) {
				if(LOG.isDebugEnabled()) {
					LOG.debug("Empty record with key: " + key.toString() + ", skiping...");
				}
				return;
			}
			context.write(key, v);
		}
	}
	
	public void extract(Path[] sourceHome, Path output) throws IOException, ClassNotFoundException, InterruptedException {
		
		if(LOG.isInfoEnabled()) {
			Log.info("ExtractPhase: extracting source: " + sourceHome);
		}
		Configuration conf = getConf() == null ? new Configuration() : getConf();
		Job job = new Job(conf, "extract " + sourceHome);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, sourceHome);
		FileOutputFormat.setOutputPath(job, output);
		
		job.setMapperClass(ExtractPhaseMapper.class);
		job.setNumReduceTasks(0);
		job.setSpeculativeExecution(false);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LamapacosArrayWritable.class);
		int ret = job.waitForCompletion(true) ? 0 : 1;
		
		if(LOG.isInfoEnabled()) {
			LOG.info("ExtractPhase: done");
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
		
		if(!args[0].equals("-extract")) {
			System.err.println("Missing operation argument:(-extract ...)");
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
		new ExtractPhase(new Configuration()).extract(list.toArray(new Path[list.size()]), new Path(output));
	}
	
	
	public static void usage() {
		System.err.println("Usage: ExtractPhase (-extract ... ) [general options]\n");
		System.err.println("* General options:");
		System.err.println();
		System.err.println("* ExtractPhase -extract <source_dir> <output> [-dir dir1 -dir dir2...]");
		System.err.println(" Extract content of a <source_dir> to <output>.\n");
		System.err.println("\t<source_dir>\tname of the souce home.");
		System.err.println("\t<output>\tname of the (non-existent) output directory.");
		System.err.println();
	}
}
