package org.lamapacos.util;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.lamapacos.io.LamapacosWritable;
import org.lamapacos.io.SegmentedContentTuple;
/**
 * WordCountSortUtil.java TODO some description 
 * wordcount and sort the word set.
 * @author root
 * @version 1.0    2013-4-8
 * 
 */
public class WordCountSortUtil extends Configured{
	
	WordCountSortUtil() {}
	WordCountSortUtil(Configuration conf) {
		setConf(conf);
	}
	public static class TokenizerMapper extends Mapper<Writable, LamapacosWritable, Text, IntWritable> {
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);
		Set<String> set = new HashSet<String>();
		StopWords stopWords = new StopWords();
		protected void setup(org.apache.hadoop.mapreduce.Mapper<Writable,LamapacosWritable,Text,IntWritable>.Context context) throws IOException ,InterruptedException {
			set.add("内容");
		};
		@Override
		public void map(Writable key, LamapacosWritable value, Context context) 
			throws IOException, InterruptedException {
			
			Writable record = value.get();
			if(record == null)
				return;
			if(record instanceof SegmentedContentTuple) {
				Text oo = ((SegmentedContentTuple) record).getSecond();
				StringTokenizer itr = new StringTokenizer(oo.toString());
				while(itr.hasMoreTokens()) {
					String s = itr.nextToken();
					if(s.indexOf("/") != -1)
						s = s.substring(0, s.lastIndexOf("/"));
					if(s == null || s.trim().equals("") || set.contains(s))
						continue;
					if(stopWords.stop(s) == true)
						continue;
					word.set(s);
					context.write(word, one);
				}	
			}
		}
	}
	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	 private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
	      public int compare(WritableComparable a, WritableComparable b) {
	        return -super.compare(a, b);
	      }
	      
	      public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	          return -super.compare(b1, s1, l1, b2, s2, l2);
	      }
	  }
	 
	public void wordCountSort(String input, String output) throws Exception {
		 Path tempDir = new Path("wordcount-temp-" + Integer.toString(
		            new Random().nextInt(Integer.MAX_VALUE))); //定义一个临时目录
		Configuration conf = getConf() == null ? new Configuration() : getConf();
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCountSortUtil.class);
		try{
			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, tempDir);//先将词频统计任务的输出结果写到临时目
            											 //录中, 下一个排序任务以临时目录为输入目录。
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			if(job.waitForCompletion(true))
			{
				Job sortJob = new Job(conf, "sort");
				sortJob.setJarByClass(WordCountSortUtil.class);
				
				FileInputFormat.addInputPath(sortJob, tempDir);
				sortJob.setInputFormatClass(SequenceFileInputFormat.class);
				
				/*InverseMapper由hadoop库提供，作用是实现map()之后的数据对的key和value交换*/
	            sortJob.setMapperClass(InverseMapper.class);
	            /*将 Reducer 的个数限定为1, 最终输出的结果文件就是一个。*/
	            sortJob.setNumReduceTasks(1); 
	            FileOutputFormat.setOutputPath(sortJob, new Path(output));
	            
	        	sortJob.setOutputKeyClass(IntWritable.class);
				sortJob.setOutputValueClass(Text.class);
				/*Hadoop 默认对 IntWritable 按升序排序，而我们需要的是按降序排列。
				 * 因此我们实现了一个 IntWritableDecreasingComparator 类,　
				 * 并指定使用这个自定义的 Comparator 类对输出结果中的 key (词频)进行排序*/
	            sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
	 
				System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
			}
		}finally{
			try {
				FileSystem.get(conf).deleteOnExit(tempDir);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	public static void main(String[] args) throws Exception {
		if(args.length < 2) {
			usage();
			return;
		}
		
		if(!args[0].equals("-wordcount")) {
			System.err.println("Missing operation argument:(-wordcount ...)");
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
		new WordCountSortUtil(new Configuration()).wordCountSort(input, output);
		
	}
	public static void usage() {
		System.err.println("Usage: WordCountSort (-worcount ... ) [general options]\n");
		System.err.println("* General options:");
		System.err.println();
		System.err.println("\t<source_dir>\tname of the souce home.");
		System.err.println("\t<output>\tname of the (non-existent) output directory.");
		System.err.println();
	}
}
