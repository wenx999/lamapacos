package org.lamapacos.util;

import java.util.HashMap;

public class StopWords {
	private static final String STOP_WORDS = "阿的了呀地得呵哈哦噢喔恩额嗯喽唏嘘";
	private static final String STOP_PUNCTUATION_MARK = "，。？！、……";
	private static HashMap<String, String[]> sentimenMap;
	
	public StopWords(){
		DictionaryUtil dicUtil = new DictionaryUtil();
		sentimenMap = new HashMap<String, String[]>();
		dicUtil.readDictionary(sentimenMap);
	}
	
	public boolean stopWord(String word){
		if (-1 == STOP_WORDS.indexOf(word))
			return false;
		return true;
	}
	
	public boolean stopPunctuationMark(String mark){
		if (-1 == STOP_PUNCTUATION_MARK.indexOf(mark))
				return false;
		return true;
	}
	
	public boolean stopSentimentWords(String sentimentWord){
		if (null == sentimenMap.get(sentimentWord))
			return false;
		return true;
	}
	
	public boolean stop(String str){
		if (stopWord(str))
			return true;
		else if (stopPunctuationMark(str))
			return true;
		else if (stopSentimentWords(str))
			return true;
		return false;
	}

}
