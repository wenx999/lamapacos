package org.lamapacos.util;

import static org.junit.Assert.assertTrue;

import java.util.StringTokenizer;

import org.junit.Test;

public class StopWordsTest {
	private static final String[] testStr ={
		new String("……"),
		new String("？"),
		new String("、"),
		new String("@"),
		new String("的"),
		new String("哦"),
		new String("好"),
		new String("脏乱"),
		new String("便宜"),
	};
	private static final String answerStr = "@好便宜";
	private StopWords stopWords = new StopWords();
	
	private static boolean equalsIgnoreWhitespace(String s1, String s2) {
		StringTokenizer st1 = new StringTokenizer(s1);
		StringTokenizer st2 = new StringTokenizer(s2);

		while (st1.hasMoreTokens()) {
			if (!st2.hasMoreTokens()) return false;
			if (!st1.nextToken().equals(st2.nextToken())) return false;
		}
		if (st2.hasMoreTokens()) return false;
		return true;
	}
	
	@Test
	public void testStop(){
		int i;
		String text = new String();
		for (i = 0; i < testStr.length; i ++ ){
			if (!stopWords.stop(testStr[i]))
				text += testStr[i];
		}
		assertTrue("expecting text: " + answerStr + System.getProperty("line.separator") + System.getProperty("line.separator") + "got text: " + text,
				equalsIgnoreWhitespace(answerStr, text));
	}
}
