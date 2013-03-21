/**
 * 
 */
package org.lamapacos.preprocessor.extraction;

import org.apache.hadoop.io.Writable;

/**
 * @author hadoop
 *
 */
public interface Extractor<S extends Writable, T extends Writable> {
	/**
	 * extract content from source, so as to get what we want
	 * for instance:get text from html
	 * @param source
	 *            text to extract
	 * @return text extract from source
	 */
	public T extract(S source);
}