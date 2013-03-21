/**
 * 
 */
package org.lamapacos.preprocessor.extraction;

/**
 * @author hadoop
 *
 */
public interface Extractor<S,T> {
	/**
	 * extract content from source, so as to get what we want
	 * for instance:get text from html
	 * @param source
	 *            text to extract
	 * @return text extract from source
	 */
	public T extract(S source);
}