/**
 * 
 */
package org.lamapacos.trainingset;

import org.lamapacos.io.TaggedContent;

/**
 * @author hadoop
 *
 */
public interface Tagger {
	public TaggedContent tag(String source);
}
