/**
 * 
 */
package org.lamapacos.trainingset;

import org.lamapacos.io.LamapacosWritable;
import org.lamapacos.io.TaggedContent;

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
}
