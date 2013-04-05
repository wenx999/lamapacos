/**
 * 
 */
package org.lamapacos.preprocessor.extraction;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.jsoup.Jsoup;
import org.jsoup.nodes.*;
import org.jsoup.select.Elements;
import org.lamapacos.io.LamapacosArrayWritable;
import org.lamapacos.io.LamapacosWritable;
import org.lamapacos.io.ScoredContent;

/**
 * parse the html pase
 * @author hadoop
 * 
 */
public class HtmlExtractor implements Extractor<Writable, Writable> {

	private Configuration conf;
	private static final String HTML_REGEX = "directly.accept.html.regex";
	private static final String HTML_REGEX_SEP = ";";
	private static final String CONTENT_SEP = "__CONT_SEP";
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
		String regexs = conf.get(HTML_REGEX, "*");
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

		List<String> list = new ArrayList<String>();
		Elements element;
		Document doc = Jsoup.parse(source.toString());
		List<LamapacosWritable> records = new ArrayList<LamapacosWritable>(); 
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

				// get the content of attribute
				if (-1 != acceptNode[i][j].indexOf("::")) {
					String[] attrsplit;
					attrsplit = acceptNode[i][j].split("::| ");
					for (int t = 1; t < attrsplit.length; t++) {
						element = doc.select(attrsplit[0]);
						k = 0;
						for (Element tmp : element) {
							Attributes attr = tmp.attributes();
							if (1 == t) {
								list.add(attrsplit[t] +"=" + attr.get(attrsplit[t])); 
							} else {
								if (k < list.size()){
									String tmpStr = list.get(k);
									list.remove(k);
									tmpStr += this.CONTENT_SEP + attr.get(attrsplit[t]); 
									list.add(k++, tmpStr);
								}else
									list.add(attrsplit[t] +"=" + attr.get(attrsplit[t])); 
							}
						}
						element.clear();
					}
				} else {// get the content of node
					element = doc.select(acceptNode[i][j]);
					k = 0;
					for (Element tmp : element) {
						if (0 == j) {
							list.add(tmp.text());
						} else {
							if (k < list.size()){
								String tmpStr = list.get(k);
								list.remove(k);
								tmpStr += this.CONTENT_SEP + tmp.text(); 
								list.add(k++, tmpStr);
							}else
								list.add(tmp.text()); 
						}
					}
					element.clear();
				}
			}// second for
			int score;
			for (j = 0; j < list.size(); j++){
				String[] split = list.get(j).split(CONTENT_SEP);
				split[0].trim();
				score = Integer.parseInt(String.valueOf(split[0].charAt(0)));
				ScoredContent scoredContent = new ScoredContent(score, split[1]);
				records.add(new LamapacosWritable(scoredContent));
			}
			list.removeAll(list); 
		}// first for 
		
		LamapacosArrayWritable ret = new LamapacosArrayWritable();      //ret is the result extract from this page.
		ret.set(records.toArray(new LamapacosWritable[records.size()]));
		return ret;
	}

}
