package org.lamapacos.trainingset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import jxl.read.biff.BiffException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.lamapacos.io.LamapacosWritable;
import org.lamapacos.io.SegmentedContentTuple;
import org.lamapacos.io.TaggedContent;
import org.lamapacos.io.TaggedWord;
import org.lamapacos.util.DictionaryUtil;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SumOfWeightTagger should extends DictBasedTagger.
 * @author hadoop
 *
 */
public class SumOfWeightTagger extends Mapper<WritableComparable, LamapacosWritable, WritableComparable, Writable> implements Tagger{
	public static final Logger LOG = LoggerFactory.getLogger(SumOfWeightTagger.class);
	public static final String WORD_SEP = " ";
	public static final char WORD_CHARACTERISTIC = '/';
	public static final int DEGREE_SCOPE = 8;
	public static final String PUNCTUATION_MARK_LABEL = "w";
	public static final String DEFAULT_SENTIMENT = "CO";
	
	private Configuration conf;
	
	public SumOfWeightTagger() {
	}
	
	public SumOfWeightTagger(Configuration conf) {
		setConf(conf);
	}
	
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
	
	public Configuration getConf() {
		return this.conf;
	}
	
	private static Map<String, String[]> dictionary;
	private static Map<String, String[]> degreeDictionary;
	@Override
	public void setup(Context context) {
		dictionary = new DictionaryUtil().readDictionary();
		try {
			degreeDictionary = new DictionaryUtil().readDegreeDictionary();
		} catch (BiffException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param key
	 * @param value
	 * @param context
	 */
	@Override
	public void map(WritableComparable key, LamapacosWritable value, Context context) 
		throws IOException, InterruptedException {
		TaggedContent taggedContent = tag(value);
		context.write(key, taggedContent);
	}
	
	
	private final List<TaggedWord> wordList = new LinkedList();
	@Override
	public  TaggedContent tag(LamapacosWritable source) {
		try{
			Writable tmptuple = source.get();
			if(tmptuple instanceof SegmentedContentTuple) {
				SegmentedContentTuple tuple = (SegmentedContentTuple)tmptuple;
				Text content = tuple.getContent();
				String segmentedContent = tuple.getSegmentedContent().toString().substring(12).trim();
				String[] words = segmentedContent.split(WORD_SEP);
				TreeMap<String,Integer> intensitySum = new TreeMap<String, Integer>();
				for(String word : words) {
					String []wordAndCh = StringUtils.split(word, ' ', WORD_CHARACTERISTIC);
					if(wordAndCh.length == 0 || wordAndCh[0] == "")
						continue;
					if(wordAndCh.length != 2) {
						throw new Exception("Corrupted Word!" + Arrays.toString(wordAndCh));
					}
					TaggedWord taggedWord = new TaggedWord();
					taggedWord.setWord(new Text(wordAndCh[0]));
					taggedWord.setCharacteristic(new Text(wordAndCh[1]));
					String[] sentimentAndIntensity = dictionary.get(wordAndCh[0]);
					if(sentimentAndIntensity != null) {
						taggedWord.setSentiment(new Text(sentimentAndIntensity[0]));
						taggedWord.setIntensity(new IntWritable(Integer.valueOf(sentimentAndIntensity[1])));
					}
					wordList.add(taggedWord);
				}
				
				//deal with degree word
				for(int i = 0; i < wordList.size(); i++) {
					TaggedWord taggedWord  = wordList.get(i);
					String[] degree = degreeDictionary.get(taggedWord.getWord().toString());
					if(degree != null) {  //a degree word
						try {
							if(taggedWord.getCharacteristic().toString().startsWith(PUNCTUATION_MARK_LABEL)) {
								throw new Exception("Word in degree Dictionary can't be punctuation mark:" + taggedWord.getWord().toString());
							}
							
							Float degreeValue = Float.valueOf(degree[0]);
							if(degreeValue < 0) {
								
							} else if(degreeValue > 0) {
								for(int j = i + 1; j < wordList.size() 
									&& !wordList.get(j).getCharacteristic().toString().startsWith(PUNCTUATION_MARK_LABEL); j++) {
									IntWritable sentimentIntensity = wordList.get(j).getIntensity();
									if(sentimentIntensity != null) {  //this word is a sentiment word
										wordList.get(j).getIntensity().set((int)(sentimentIntensity.get() * degreeValue));
									}
								}
							}
						} catch(NumberFormatException nfe) {
							throw new NumberFormatException("Degree of word:" + taggedWord.getWord() + " is corrupted!");
						} catch (Exception e) {
							e.printStackTrace();
						}
					} else if (taggedWord.getSentiment() != null) { // a sentiment word
						String sentiment = taggedWord.getSentiment().toString();
						Integer sum = null;
						if(intensitySum.containsKey(sentiment)) {
							sum = intensitySum.get(sentiment);
						} else {
							sum = new Integer(0);
						}
						sum += taggedWord.getIntensity().get();
						intensitySum.put(sentiment, sum);
					}
				}
				Integer maxFreq = Integer.MIN_VALUE;
				String maxSentiment = null;
				for(Entry<String, Integer> entry : intensitySum.entrySet()) {
					if(entry.getValue() > maxFreq) {
						maxFreq = entry.getValue();
						maxSentiment = entry.getKey();
					}
				}
				if(maxSentiment != null) {
					return new TaggedContent(new Text(content), new Text(maxSentiment));
				}
				else {
					return new TaggedContent(new Text(content), new Text(DEFAULT_SENTIMENT));
				}
			}else {
				throw new Exception("Wrong type" + tmptuple.getClass().getCanonicalName());
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			wordList.clear();
		}
		return null;
	}
	
	/**
	 * tag phase, tag all K-V pair
	 * @param sourceHome
	 * @param output
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public void tagAll(Path[] sourceHome, Path output) throws IOException, InterruptedException, ClassNotFoundException {
		if(LOG.isInfoEnabled()) {
			Log.info("TagPhase: taging source:" + sourceHome);
		}
		
		Configuration conf = getConf() == null ? new Configuration():getConf();
		Job job = new Job(conf, "tag " + sourceHome);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, sourceHome);
		FileOutputFormat.setOutputPath(job, output);
		job.setMapperClass(SumOfWeightTagger.class);
		job.setNumReduceTasks(0);
		job.setSpeculativeExecution(false);
		job.setMapOutputKeyClass(Writable.class);
		job.setMapOutputValueClass(TaggedContent.class);
		int ret = job.waitForCompletion(true) ? 0:1;
		if(LOG.isInfoEnabled()) {
			LOG.info("TagPhase: done");
		}
		System.exit(ret);
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length < 2) {
			usage();
			return;
		}
		
		if(!args[0].equals("-tag")) {
			System.err.println("Missing operation argument:(-tag ...)");
			usage();
			return;
		}
		
		String input = args[1];
		if(null == input) {
			System.err.println("Missing required argument: <source_dir>");
			usage();
			return;
		}
		String output = args.length > 2 ? args[2] : null;
		if(null == output) {
			System.err.println("Missing required argument: <output>");
			usage();
			return;
		}
		List<Path> list = new ArrayList<Path>();
		list.add(new Path(input));
		if(args.length > 3) {
			if(args.length%2 == 1) {
				for(int i = 2; i < args.length; i++) {
					if(args[i].equals("-dir")) {
						list.add(new Path(args[i++]));
					}
				}
			}else {
				usage();
				return;
			}
		}
		new SumOfWeightTagger(new Configuration()).tagAll(list.toArray(new Path[list.size()]), new Path(output));
	}
	
	public static void usage() {
		System.err.println("Usage: TagPhase (-tag ... ) [general options]\n");
		System.err.println("* General options:");
		System.err.println();
		System.err.println("* TagPhase -tag <source_dir> <output> [-dir dir1 -dir dir2...]");
		System.err.println("  tag content of a <source_dir> to <output>.");
		System.err.println("  Each output contains the original content and its tag, which indicate the opinion.\n");
		System.err.println("\t<source_dir>\tname of the souce home.");
		System.err.println("\t<output>\tname of the (non-existent) output directory.");
		System.err.println();
	}
}