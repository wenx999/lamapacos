package org.lamapacos.trainingset;

import org.apache.hadoop.conf.Configured;
import org.lamapacos.io.TaggedContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SumOfWeightTagger should extends DictBasedTagger.
 * @author hadoop
 *
 */
public class SumOfWeightTagger extends Configured implements Tagger{
	public static final Logger LOG = LoggerFactory.getLogger(SumOfWeightTagger.class);
	
	public SumOfWeightTagger() {
	}
	
	public SumOfWeightTagger(Configuration conf) {
		setConf(conf);
	}
	
	public static class SumOfWeightTaggerMapper
		extends Mapper<WritableComparable key, Writable value, >
	@Override
	public TaggedContent tag(String source) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
