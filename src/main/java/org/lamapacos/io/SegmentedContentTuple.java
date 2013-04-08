/**
 * 
 */
package org.lamapacos.io;

import org.apache.hadoop.io.Text;

/**
 * @author hadoop
 *
 */
public class SegmentedContentTuple extends TwoTuple<Text, Text> {

	public SegmentedContentTuple() {
		super();
		this.first = new Text();
		this.second = new Text();
	}
	
	public SegmentedContentTuple(Text content, Text segmentedContent) {
		super(content, segmentedContent);
	}
	
	/**
	 * @return the content
	 */
	public Text getContent() {
		return this.getFirst();
	}

	/**
	 * @param content the content to set
	 */
	public void setContent(Text content) {
		this.setFirst(content);
	}

	/**
	 * @return the segmentedContent
	 */
	public Text getSegmentedContent() {
		return this.getSecond();
	}

	/**
	 * @param segmentedContent the segmentedContent to set
	 */
	public void setSegmentedContent(Text segmentedContent) {
		this.setSecond(segmentedContent);
	}
}
