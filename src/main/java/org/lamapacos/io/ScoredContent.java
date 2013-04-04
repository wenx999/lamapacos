/**
 * 
 */
package org.lamapacos.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author hadoop
 *
 */
public final class ScoredContent implements Writable {
	private int score;
	private String content;
	
	public ScoredContent() {}
	public ScoredContent(int score, String content) {
		if(content == null)
			throw new IllegalArgumentException("score");
		this.score = score;
		this.content = content;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(score);
		Text.writeString(out, content);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.score = in.readInt();
		this.content = Text.readString(in);
	}
	
	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof ScoredContent))
			return false;
		ScoredContent tmp = (ScoredContent)obj;
		return this.score != tmp.score ? false: this.content.equals(tmp.content); 
	}
	
	@Override
	public int hashCode() {
		int ret = 31;
		ret += score*7;
		ret += this.content.hashCode()*7;
		return ret;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ScoredContent:\n");
		builder.append("[score=");
		builder.append(this.score);
		builder.append(",content=");
		builder.append(this.content);
		builder.append("]");
		return builder.toString();
	}
	public int getScore() {
		return score;
	}
	public void setScore(int score) {
		this.score = score;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
}
