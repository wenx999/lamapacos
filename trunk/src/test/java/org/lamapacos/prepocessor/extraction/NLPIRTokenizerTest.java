/**
 * 
 */
package org.lamapacos.prepocessor.extraction;

import org.junit.Before;
import org.junit.Test;
import org.lamapacos.preprocessor.extraction.NLPIRTokenizer;
import org.lamapacos.preprocessor.extraction.Tokenizer;

/**
 * @author hadoop
 *
 */
public class NLPIRTokenizerTest {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		System.out.println(System.getProperty("java.library.path"));
	}

	/**
	 * Test method for {@link org.lamapacos.preprocessor.extraction.NLPIRTokenizer#segment(java.lang.String)}.
	 */
	@Test
	public void testSegment() {
		Tokenizer tokenizer = new NLPIRTokenizer();
		System.out.println(tokenizer.segment("并非美丽和漂亮，你真的很好"));
	}

}
