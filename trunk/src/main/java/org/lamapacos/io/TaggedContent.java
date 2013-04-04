/**
 * 
 */
package org.lamapacos.io;

import org.apache.hadoop.io.Text;

/**
 * @author hadoop
 *
 */
public class TaggedContent extends TwoTuple<Text, Text>{
	/**
	 * default constructor
	 */
	public TaggedContent() {
		super();
	}
	
	/**
	 * constructor
	 * @param content
	 * @param tag
	 */
	public TaggedContent(Text content, Text tag) {
		super(content, tag);
	}
	
	/**
	 * @return the content
	 */
	public Text getContent() {
		return this.first;
	}
	/**
	 * @param content the content to set
	 */
	public void setContent(Text content) {
		this.first = content;
	}
	/**
	 * @return the tag
	 */
	public Text getTag() {
		return this.second;
	}
	
	/**
	 * @param tag the tag to set
	 */
	public void setTag(Text tag) {
		this.second = tag;
	}

}
