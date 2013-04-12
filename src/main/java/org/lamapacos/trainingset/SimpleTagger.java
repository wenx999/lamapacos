package org.lamapacos.trainingset;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import jxl.read.biff.BiffException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.lamapacos.io.LamapacosWritable;
import org.lamapacos.io.SegmentedContentTuple;
import org.lamapacos.io.TaggedContent;
import org.lamapacos.io.TaggedWord;
import org.lamapacos.util.Constant;
import org.lamapacos.util.DictionaryUtil;

public class SimpleTagger extends Configured implements Tagger {
	private static Map<String, String[]> dictionary;
	private static Map<String, String[]> degreeDic;
	private static Map<String, String[]> tagmapDic;
	private static List<TaggedWord> wordList;

	public static final String WORD_SEP = " ";
	public static final char WORD_CHARACTERISTIC = '/';
	public static final int DEGREE_SCOPE = 6;
	public static final String PUNCTUATION_MARK_LABEL = "w";
	public static final String DEFAULT_SENTIMENT = "CO";

	public SimpleTagger() {
		setConf(new Configuration());
		getConf().addResource("lamapacos_preprocessor.xml");
		setup();
	}

	public SimpleTagger(Configuration conf) {
		setConf(conf);
		setup();
	}

	public void setup() {
		wordList = new LinkedList<TaggedWord>();
		try {
			dictionary = new DictionaryUtil(getConf()).readDictionary();
			degreeDic = new DictionaryUtil(getConf()).readDegreeDictionary();
			tagmapDic = new DictionaryUtil(getConf()).readTagMapDictionary();
		} catch (BiffException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public TaggedContent tag(LamapacosWritable source) {
		TreeMap<String, Integer> intensitySum = new TreeMap<String, Integer>();
		Writable tmptuple = source.get();
		try{
		if (tmptuple instanceof SegmentedContentTuple) {
			SegmentedContentTuple tuple = (SegmentedContentTuple) tmptuple;
			Text content = tuple.getContent();
			String segmentedContent = tuple.getSegmentedContent().toString().substring(12).trim();
			String[] words = segmentedContent.split(WORD_SEP);
			
			featureTag(words);
			parsing();
			//count every sentiment tag's value
			for (int i = 0; i < wordList.size(); i++) {
				String sentiment = wordList.get(i).getSentiment().toString();
				//first location is the father tag, and the other is the children tag
				String[] split = sentiment.toString().split(Constant.CONTENT_SEP);
				Integer sum = null;
				if( 1 != split.length ){ 
					// the label of sentiment is a father tag, it has any children tag.
					for (int j = 0; j < split.length; j ++){
						if (intensitySum.containsKey(split[j])) {
							sum = intensitySum.get(split[j]);
							sum += wordList.get(i).getIntensity().get();
							intensitySum.put(sentiment, sum);
						} else if (0 == j){
							// put the father tag into the TreeMap<String, Integer>,if it doesn't exit.
							sum = wordList.get(i).getIntensity().get();
							intensitySum.put(split[0], sum);
						}
					}
					wordList.get(i).setSentiment(new Text(split[0]));// reset the tag of the sentiment to a father class
				}else if (intensitySum.containsKey(sentiment)) {
					sum = intensitySum.get(sentiment);
					sum += wordList.get(i).getIntensity().get();
					intensitySum.put(sentiment, sum);
				} else {
					sum = wordList.get(i).getIntensity().get();
					intensitySum.put(sentiment, sum);
				}
				
			}
			
			Integer maxFreq = Integer.MIN_VALUE;
			String maxSentiment = null;
			//look for the max value of sentiment tag
			for (Entry<String, Integer> entry : intensitySum.entrySet()) {
				if (entry.getValue() > maxFreq) {
					maxFreq = entry.getValue();
					maxSentiment = entry.getKey();
				}
			}
			if (maxSentiment != null) {
				return new TaggedContent(new Text(content), new Text(maxSentiment +":"+ maxFreq));
			} else {
				return new TaggedContent(new Text(content), new Text(DEFAULT_SENTIMENT));
			}
		} else {
			throw new Exception("Wrong type" + tmptuple.getClass().getCanonicalName());
		}
		}catch (Exception e){
			e.printStackTrace();
		}finally{
			wordList.clear();
		}
		return null;
	}

	@Override
	public void featureTag(String[] words) {
		for (String word : words) {
			// spilt the string to word and characteristic of this word
			String[] wordAndCh = StringUtils.split(word, ' ', WORD_CHARACTERISTIC);
			if (wordAndCh.length == 0 || wordAndCh[0] == "") continue;
			if (wordAndCh.length != 2) {
				try {
					throw new Exception("Corrupted Word!" + Arrays.toString(wordAndCh));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			// tag the word by FourTuple<Text, Text, Text, IntWritable>
			TaggedWord taggedWord = new TaggedWord();
			taggedWord.setWord(new Text(wordAndCh[0]));
			taggedWord.setCharacteristic(new Text(wordAndCh[1]));
			String[] sentimentAndIntensity = dictionary.get(wordAndCh[0]);
			if (sentimentAndIntensity != null) {
				taggedWord.setSentiment(new Text(sentimentAndIntensity[0]));
				taggedWord.setIntensity(new IntWritable(Integer.valueOf(sentimentAndIntensity[1])));
			} else {// if it is not a sentiment word, it will be set a label of "CO"
					// and it's value is zero
				taggedWord.setSentiment(new Text(DEFAULT_SENTIMENT));
				taggedWord.setIntensity(new IntWritable(Integer.valueOf(0)));
			}
			wordList.add(taggedWord);
		}

	}

	@Override
	public void parsing() {
		for (int i = 0; i < wordList.size(); i++) {
			TaggedWord taggedWord = wordList.get(i);
			String[] degree = degreeDic.get(taggedWord.getWord().toString());
			if (degree != null) { // a degree word
				try {
					if (taggedWord.getCharacteristic().toString().startsWith(PUNCTUATION_MARK_LABEL)) { throw new Exception(
							"Word in degree Dictionary can't be punctuation mark:" + taggedWord.getWord().toString()); }

					Float degreeValue = Float.valueOf(degree[0]);
					if (degreeValue < 0) {
						for (int j = i + 1; j < wordList.size() && j < DEGREE_SCOPE 
								&& !wordList.get(j).getCharacteristic().toString().startsWith(PUNCTUATION_MARK_LABEL); j++) {
							Text sentiment = wordList.get(j).getSentiment();
							if ( sentiment != null && !sentiment.toString().equalsIgnoreCase(DEFAULT_SENTIMENT)) { // this word is a sentiment word
								String[] tagmap = tagmapDic.get(sentiment.toString());
								//tagmap[0] is a children tag of sentiment
								if ( tagmap[1].equalsIgnoreCase("") || null == tagmap[1]){
									wordList.get(j).setSentiment(new Text(tagmap[0]));
								}else{
									//tagmap[0] is a father tag of sentiment, make a new tag:(father tag) +"," +(children tag)
									wordList.get(j).setSentiment(new Text(tagmap[0] +","+ tagmap[1]));
								}
							}
						}
					} else if (degreeValue > 0) {
						for (int j = i + 1; j < wordList.size() 
								&& !wordList.get(j).getCharacteristic().toString().startsWith(PUNCTUATION_MARK_LABEL); j++) {
							IntWritable sentimentIntensity = wordList.get(j).getIntensity();
							if (sentimentIntensity != null) { // this word is a sentiment word
								wordList.get(j).getIntensity().set((int) (sentimentIntensity.get() * degreeValue));
							}
						}
					}
				} catch (NumberFormatException nfe) {
					throw new NumberFormatException("Degree of word:" + taggedWord.getWord() + " is corrupted!");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

}
