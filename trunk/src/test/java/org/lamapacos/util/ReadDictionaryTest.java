package org.lamapacos.util;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.junit.Test;


public class ReadDictionaryTest {
	DictionaryUtil readDic = new DictionaryUtil();

	private static final String answerText =  new String("赏信罚明PH5  脏乱NN7  便宜PH5  好PH7  敏悟PH3  ");

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
	public void textExtractor() {
		HashMap<String, String[]> hashmap;
		hashmap = readDic.readDictionary();
		Iterator<Entry<String, String[]>> iter = hashmap.entrySet().iterator();
		String text = new String();
		while (iter.hasNext()) {
			@SuppressWarnings("rawtypes")
			Map.Entry entry = (Map.Entry) iter.next();
		    Object key = entry.getKey();
		    String[] val = (String[])entry.getValue();
		    text += key + val[0] + val[1] +"  "; 
		} 
		assertTrue("expecting text: " + answerText + System.getProperty("line.separator") + System.getProperty("line.separator") + "got text: " + text,
					equalsIgnoreWhitespace(answerText, text));

	}
}
