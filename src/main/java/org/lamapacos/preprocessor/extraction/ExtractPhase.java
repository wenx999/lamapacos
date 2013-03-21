/**
 * 
 */
package org.lamapacos.preprocessor.extraction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hadoop
 *
 */
public class ExtractPhase extends Configured{
	public static final Logger LOG = LoggerFactory.getLogger(ExtractPhase.class);
	
	public static class ExtractPhaseMapper 
		extends Mapper<WritableComparable, Writable, Writable, Writable> {
		private Text newKey = new Text();
		Extractor extractor = new HtmlExtractor();
		@Override
		public void map(WritableComparable key, Writable value, Context context)
			throws IOException, InterruptedException {
			// convert on the fly from old formats with UTF8 keys.
			// UTF8 deprecated and replaced by Text.
			if(key instanceof Text) {
				newKey.set(key.toString());
				key = newKey;
			}
			context.write(key, extractor.extract(value));
		}
	}
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if(args.length < 2) {
			usage();
			return;
		}
		
		if(!args[0].equals("-extract")) {
			System.err.println("Missing operation argument:(-extract ...)");
			usage();
			return;
		}
		
		Configuration conf = new Configuration();
		final FileSystem fs = FileSystem.get(conf);
	}
	
	
	public static void usage() {
		System.err.println("Usage: ExtractPhase (-extract ... ) [general options]\n");
		System.err.println("* General options:");
		System.err.println();
		System.err.println("* ExtractPhase -extract <source_dir> <output> [general options]");
		System.err.println("Extract content of a <source_dir> to <output>.\n");
		System.err.println("\t<source_dir>\tname of the souce home.");
		System.err.println("\t<output>\tname of the (non-existent) output directory.");
		System.err.println();
	}
}
