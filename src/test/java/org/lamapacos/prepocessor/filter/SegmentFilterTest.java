/**
 * 
 */
package org.lamapacos.prepocessor.filter;

import static org.junit.Assert.*;

import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.lamapacos.preprocessor.filter.SegmentFilter;
import org.lamapacos.preprocessor.filter.SegmentFilter.RegexsFilter;

/**
 * @author hadoop
 *
 */
public class SegmentFilterTest {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testRegexFilter() {
		Configuration conf = new Configuration();
		conf.addResource("lamapacos-preprocessor.xml");
		
		SegmentFilter.RegexsFilter filter = new SegmentFilter.RegexsFilter();
		RegexsFilter.setPatterns(conf);
		assertTrue(filter.accept("www.1mall.91ka.com"));		
	}
	/**
	 * Test method for {@link org.lamapacos.preprocessor.filter.SegmentFilter#dump(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path)}.
	 */
	@Test
	public void testDump() {
//		fail("Not yet implemented");
	}

}
