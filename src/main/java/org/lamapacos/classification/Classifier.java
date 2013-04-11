package org.lamapacos.classification;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lamapacos.preprocessor.extraction.ExtractPhase;
import org.lamapacos.preprocessor.extraction.Splitter;
import org.lamapacos.preprocessor.filter.SegmentFilter;
import org.lamapacos.trainingset.SumOfWeightTagger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the classifer's process stream, it's include url filter,
 * parse html page,split words,make a sentiment tag  
 * @version 1.0    Apr 10, 2013
 *
 */
public class Classifier extends Configured implements Tool {
	public static final Logger LOG = LoggerFactory.getLogger(Classifier.class);

	public static void main(String args[]) throws Exception {
		
		if (args.length < 1) {
			usage();
			return;
		}
		if(!args[0].equals("-classifier")) {
			System.err.println("Missing operation argument:(-classifier ...)");
			usage();
			return;
		}
		String input = args[1];
		if (input == null) {
			System.err.println("Missing required argument: <source_dir>");
			usage();
			return;
		}
		String output = args.length > 2 ? args[2] : null;
		if (output == null) {
			System.err.println("Missing required argument: <output>");
			usage();
			return;
		}
		
		Configuration conf = new Configuration();
		conf.addResource("lamapacos-preprocessor.xml");
		int res = ToolRunner.run(conf, new Classifier(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Path inputDir = new Path(args[1]);
		Path outputdir = new Path(args[2]);
		Path urlFilter = new Path(outputdir + "/urlFilter");
		Path extract = new Path(outputdir + "/extract");
		Path contentSplit = new Path(outputdir + "/contentSplit");
		Path trainingSet = new Path(outputdir + "/trainingSet");
		
		SegmentFilter segFilter = new SegmentFilter(getConf());
		ExtractPhase extrPhase = new ExtractPhase(getConf());
		Splitter splitter = new Splitter(getConf());
		SumOfWeightTagger tagger = new SumOfWeightTagger(getConf());
			
		segFilter.dump(inputDir, urlFilter);//url filter
				
		extrPhase.extract(urlFilter, extract);//parse html page	
		
		splitter.split(extract, contentSplit);	//split words
		
		tagger.tagAll(contentSplit, trainingSet);//make a sentiment tag for a content record
		
		if (LOG.isInfoEnabled()) { LOG.info("classifier finished: " + outputdir); }
		return 0;
	}
	
	public static void usage() {
		System.err.println("Usage: Classifier (-classifier ... ) [general options]\n");
		System.err.println("* General options:");
		System.err.println();
		System.err.println("* Classifier -classifier <source_dir> <output>");
		System.err.println(" Classifier content of a <source_dir> to <output>.\n");
		System.err.println("\t<source_dir>\tname of the souce home.");
		System.err.println("\t<output>\tname of the (non-existent) output directory.");
		System.err.println();
	}
}
