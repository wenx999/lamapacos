package org.lamapacos.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TwoTuple<S extends Writable,T extends Writable> implements Writable{
	protected S first;
	protected T second;

	public TwoTuple() {
	}
	
	public TwoTuple(S first, T second) {
		this.first = first;
		this.second = second;
	}
	
	@Override
	public int hashCode() {
		int ret = 31;
		ret += ret * 7 + first.hashCode();
		ret += ret * 7 + second.hashCode();
		return ret;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof TwoTuple))
			return false;
		@SuppressWarnings("rawtypes")
		TwoTuple tmp = (TwoTuple)obj;
		return this.first.equals(tmp.getFirst()) ? this.second.equals(tmp.getSecond()) : false;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("[first:");
		builder.append(first.toString());
		builder.append(",second:");
		builder.append(second.toString());
		builder.append("]");
		return builder.toString();
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}
	
	public Writable getFirst() {
		return this.first;
	}
	public void setScore(S first) {
		this.first = first;
	}
	public Writable getSecond() {
		return second;
	}
	public void setSecond(T second) {
		this.second = second;
	}
	
}
