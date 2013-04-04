/**
 * 
 */
package org.lamapacos.io;

import org.apache.hadoop.io.Text;

/**
 * @author hadoop
 *
 */
public class TaggedContent {
	private Text content;
	private Text tag;
	/**
	 * default constructor
	 */
	public TaggedContent() {
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * constructor
	 * @param content
	 * @param tag
	 */
	public TaggedContent(Text content, Text tag) {
		this.content = content;
		this.tag = tag;
	}
	
	/**
	 * @return the content
	 */
	public Text getContent() {
		return content;
	}
	/**
	 * @param content the content to set
	 */
	public void setContent(Text content) {
		this.content = content;
	}
	/**
	 * @return the tag
	 */
	public Text getTag() {
		return tag;
	}
	
	/**
	 * @param tag the tag to set
	 */
	public void setTag(Text tag) {
		this.tag = tag;
	}

}
