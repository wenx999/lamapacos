/**
 * 
 */
package org.lamapacos.preprocessor.extraction;


/**
 * @author hadoop
 *
 */
public interface Tokenizer {
	public String segment(String source);
}

class PseudoTokenizer implements Tokenizer{
	public PseudoTokenizer() {}
	public String segment(String source) {
		return source;
	}
}