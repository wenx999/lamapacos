/**
 * 
 */
package org.lamapacos.preprocessor.extraction;

import org.apache.hadoop.io.ArrayWritable;


/**
 * @author hadoop
 *
 */
public class LamapacosArrayWritable extends ArrayWritable{

	public LamapacosArrayWritable() {
		super(LamapacosWritable.class);
	}

}
