/**
 * 
 */
package org.lamapacos.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * @author hadoop
 *
 */
public class TaggedWord extends FourTuple<Text, Text, Text, IntWritable> {
	public TaggedWord() {}
	public TaggedWord(Text word, Text characteristic, Text sentiment, IntWritable intensity) {
		this.first = word;
		this.second = characteristic;
		this.third = sentiment;
		this.fourth = intensity;
	}
	
	public Text getWord() {
		return this.first;
	}
	
	public void setWord(Text word) {
		this.first = word;
	}
	
	public Text getCharacteristic() {
		return this.second;
	}
	
	public void setCharacteristic(Text characteristic) {
		this.second = characteristic;
	}
	
	public Text getSentiment() {
		return this.third;
	}
	
	public void setSentiment(Text sentiment) {
		this.third = sentiment;
	}
	
	public IntWritable getIntensity() {
		return this.fourth;
	}
	
	public void setIntensity(IntWritable intensity) {
		this.fourth = intensity;
	}
}
