/**
 * 
 */
package org.lalapacos.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.lamapacos.io.TwoTuple;

/**
 * @author hadoop
 *
 */
public class TwoTupleTest {
	TwoTuple<Text,Text> twoTuple;
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		twoTuple = new TwoTuple<Text,Text>(new Text("first"), new Text("second"));
	}

	/**
	 * Test method for {@link org.lamapacos.io.TwoTuple#equals(java.lang.Object)}.
	 */
	@Test
	public void testEqualsObject() {
		assertTrue(twoTuple.equals(new TwoTuple<Text,Text>(new Text("first"), new Text("second"))));
	}

	/**
	 * Test method for {@link org.lamapacos.io.TwoTuple#toString()}.
	 */
	@Test
	public void testToString() {
		assertEquals(twoTuple.toString(), "[first:first,second:second]");
		assertNotNull(twoTuple.toString());
		assertFalse(twoTuple.toString().equals("goodbye"));
	}

	/**
	 * Test method for {@link org.lamapacos.io.TwoTuple#readFields(java.io.DataInput)}.
	 */
	@Test
	public void testReadFields() {
		DataOutput out = new DataOutputStream(new BufferedOutputStream(new ByteArrayOutputStream()));
		try {
			twoTuple.write(out);
			
			DataInput in = new DataInputStream(new BufferedInputStream(new ByteArrayInputStream(out.toString().getBytes())));
			TwoTuple<Text, Text> tmpTwoTuple = new TwoTuple<Text, Text>(new Text(), new Text());
			tmpTwoTuple.readFields(in);
			
			assertEquals(tmpTwoTuple, twoTuple);
		} catch (IOException e) {
			//ignore exception
		}
	}

	/**
	 * Test method for {@link org.lamapacos.io.TwoTuple#write(java.io.DataOutput)}.
	 */
	@Test
	public void testWrite() {
		
		DataOutput out = new DataOutputStream(new ByteArrayOutputStream());
		try {
			twoTuple.write(out);
			DataInput in = new DataInputStream(new ByteArrayInputStream(out.toString().getBytes()));
			TwoTuple<Text, Text> tmpTwoTuple = new TwoTuple<Text, Text>(new Text(), new Text());
			tmpTwoTuple.readFields(in);
			assertEquals(tmpTwoTuple, twoTuple);			
		} catch (IOException e) {
			//ignore exception
		}
	}

}
