/**
 * 
 */
package org.lamapacos.prepocessor.extraction;

import java.util.Arrays;
import java.util.StringTokenizer;

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
		
		tokenizer.segment("并非不美丽和漂亮~~。》>三人行，,你真的很好.");
	
		
	}
	
//	@Test
//	public void testStringTokenizer() {
//		StringTokenizer fenxi = new StringTokenizer("We, are  ;;; student",",;");
//		System.out.println(fenxi.countTokens());
//		while(fenxi.hasMoreElements())
//		System.out.println(fenxi.nextElement());
//	}

}
