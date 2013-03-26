/**
 * 
 */
package org.lamapacos.preprocessor.extraction;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.jsoup.Jsoup;
import org.jsoup.nodes.*;
import org.jsoup.select.Elements;

/**
 * @author hadoop
 * 
 */
public class HtmlExtractor implements Extractor<Writable, Writable> {

	private Configuration conf;
	private static final String HTML_REGEX = "directly.accept.html.regex";
	private static final String HTML_REGEX_SEP = ";";
	private static final String CONTENT_SEP = "__CONT_SEP";
	private static final String CONTENT_END = "__CONT_END";
	private static String[][] acceptNode;

	@SuppressWarnings("static-access")
	public HtmlExtractor() {
		conf = new Configuration();
		this.addResource();
		this.setPatterns(conf);
	}

	@SuppressWarnings("static-access")
	public HtmlExtractor(Configuration conf) {
		this.conf = conf;
		this.addResource();
		this.setPatterns(conf);
	}

	public void setConfiguration(Configuration conf) {
		this.conf = conf;
	}

	public Configuration getConfiguration() {
		return this.conf;
	}

	public void addResource() {
		this.conf.addResource("lamapacos-preprocessor.xml");
	}

	public static void setPatterns(Configuration conf) {
		int i;
		String regexs = conf.get(HTML_REGEX, "html");
		String[] split = regexs.split(HTML_REGEX_SEP+HTML_REGEX_SEP);
		acceptNode = new String[split.length][];
		for (i = 0; i < split.length; i++) {
			acceptNode[i] = split[i].split(";");
		}

	}

	@SuppressWarnings("static-access")
	@Override
	public Writable extract(Writable source) {
		int i, j, k;
		StringBuilder result = new StringBuilder("");
		List<String> list = new ArrayList<String>();
		Elements element;
		Document doc = Jsoup.parse(source.toString());
		// //test
		// element = doc.select("strong.scoring  p");
		// System.out.print(element.size());
		// for (Element tmp: element){
		//
		// result.append(tmp.text());
		// }

		for (i = 0; i < acceptNode.length; i++) {

			for (j = 0; j < acceptNode[i].length; j++) {
				// look for the childnode: acceptNode[i][j] which predefind in the conf
				String[] split;
				split = acceptNode[i][j].split("@");
				String nodeName;

				// if no rename the node, the defautl name will be acceptNode[i][j];
				if (split.length < 2) {
					nodeName = acceptNode[i][j] + ":";
				} else {
					nodeName = split[1] + ":";
				}
				

				// get the content of attribute
				if (-1 != split[0].indexOf("::")) {
					String[] attrsplit;
					attrsplit = split[0].split("::| ");
					for (int t = 1; t < attrsplit.length; t++) {
						element = doc.select(attrsplit[0]);
						k = 0;
						for (Element tmp : element) {
							Attributes attr = tmp.attributes();
							if (1 == t) {
								list.add(nodeName + attrsplit[t] +"=" + attr.get(attrsplit[t]));
							} else {
								if (k < list.size()){
									String tmpStr = list.get(k);
									list.remove(k);
									tmpStr += this.CONTENT_SEP + attrsplit[t] +"=" + attr.get(attrsplit[t]);
									list.add(k++, tmpStr);
								}else
									list.add(nodeName + attrsplit[t] +"=" + attr.get(attrsplit[t]));
							}
						}
						element.clear();
					}
				} else {// get the content of node
					element = doc.select(split[0]);
					k = 0;
					for (Element tmp : element) {
						if (0 == j) {
							list.add(nodeName + tmp.text());
						} else {
							if (k < list.size()){
								String tmpStr = list.get(k);
								list.remove(k);
								tmpStr += this.CONTENT_SEP + nodeName + tmp.text();
								list.add(k++, tmpStr);
							}else
								list.add(nodeName + tmp.text());
						}
					}
					element.clear();
				}
			}// second for
			for (j = 0; j < list.size(); j++)
				result.append(list.get(j) + this.CONTENT_END + "\n");
			list.removeAll(list); 
		}// first for 
		return new Text(result.toString());
	}

}
