/**
 * 
 */
package org.lamapacos.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author hadoop
 *
 */
public class FourTuple <S extends Writable, T extends Writable, U extends Writable, V extends Writable> implements Writable{
	protected S first;
	protected T second;
	protected U third;
	protected V fourth;
	
	public FourTuple() {
		super();
	}
	
	/**
	 * @param first
	 * @param second
	 * @param third
	 * @param fourth
	 */
	public FourTuple(S first, T second, U third, V fourth) {
		super();
		this.first = first;
		this.second = second;
		this.third = third;
		this.fourth = fourth;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((fourth == null) ? 0 : fourth.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		result = prime * result + ((third == null) ? 0 : third.hashCode());
		return result;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof FourTuple))
			return false;
		FourTuple other = (FourTuple) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (fourth == null) {
			if (other.fourth != null)
				return false;
		} else if (!fourth.equals(other.fourth))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		if (third == null) {
			if (other.third != null)
				return false;
		} else if (!third.equals(other.third))
			return false;
		return true;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "FourTuple [first=" + first + ", second=" + second + ", third="
				+ third + ", fourth=" + fourth + "]";
	}
	
	/**
	 * @return the first
	 */
	public S getFirst() {
		return first;
	}
	/**
	 * @param first the first to set
	 */
	public void setFirst(S first) {
		this.first = first;
	}
	/**
	 * @return the second
	 */
	public T getSecond() {
		return second;
	}
	/**
	 * @param second the second to set
	 */
	public void setSecond(T second) {
		this.second = second;
	}
	/**
	 * @return the third
	 */
	public U getThird() {
		return third;
	}
	/**
	 * @param third the third to set
	 */
	public void setThird(U third) {
		this.third = third;
	}
	/**
	 * @return the fourth
	 */
	public V getFourth() {
		return fourth;
	}
	/**
	 * @param fourth the fourth to set
	 */
	public void setFourth(V fourth) {
		this.fourth = fourth;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
		third.readFields(in);
		fourth.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
		third.write(out);
		fourth.write(out);
	}
}