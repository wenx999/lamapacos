/**
 * 
 */
package org.lamapacos.trainingset;

import java.util.List;

import org.lamapacos.io.LamapacosWritable;
import org.lamapacos.io.TaggedContent;
import org.lamapacos.io.TaggedWord;

/**
 * @author hadoop
 *
 */	
public interface Tagger {
	/*****
	 * tag a k-v pair, which indicate the content's opinion
	 * @param source
	 * @return
	 */
	public TaggedContent tag(LamapacosWritable source);
	
	public void featureTag(String[] words);
	
	public void parsing();
}
