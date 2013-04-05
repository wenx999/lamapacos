/**
 * 
 */
package org.lamapacos.io;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

/**
 * @author hadoop
 *
 */
@SuppressWarnings("unchecked")
public class LamapacosWritable extends GenericWritable{
	private static Class<? extends Writable>[] CLASSES = null;
	static {
		CLASSES = new Class[] {
				org.apache.hadoop.io.NullWritable.class,
				org.apache.hadoop.io.BooleanWritable.class,
				org.apache.hadoop.io.LongWritable.class,
			    org.apache.hadoop.io.BytesWritable.class,
			    org.apache.hadoop.io.FloatWritable.class,
			    org.apache.hadoop.io.IntWritable.class,
			    org.apache.hadoop.io.MapWritable.class,
			    org.apache.hadoop.io.Text.class,
			    org.apache.hadoop.io.MD5Hash.class,
			    ScoredContent.class,
			    TwoTuple.class,
			    SegmentedContentTuple.class,
			    TaggedContent.class
		};
	}
	
	public LamapacosWritable(){}
	public LamapacosWritable(Writable instance) {
		set(instance);
	}
	
	@Override
	protected Class<? extends Writable>[] getTypes() {
		return CLASSES;
	}

}
