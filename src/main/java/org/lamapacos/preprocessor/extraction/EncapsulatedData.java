package org.lamapacos.preprocessor.extraction;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.lamapacos.io.LamapacosArrayWritable;
import org.lamapacos.io.LamapacosWritable;
import org.lamapacos.io.ScoredContent;
import org.lamapacos.util.Constant;

public class EncapsulatedData extends Configured {
	public EncapsulatedData() {
		setConf(getConf() == null ? new Configuration() : getConf());
	}

	public EncapsulatedData(Configuration conf) {
		setConf(conf);
	}

	public void encapsulated(List<LamapacosWritable> records, List<String> list) throws ClassNotFoundException, InstantiationException, IllegalAccessException{
		Configuration conf = getConf();
		String className = conf.get(Constant.EXTRACT_DATASTRU_REGEX, Constant.DEFAULT_EXTDATASTR_VALUE);
		Class cs = Class.forName(className);
		Writable writable = (Writable) cs.newInstance();
		LamapacosArrayWritable ret = new LamapacosArrayWritable();      //ret is the result extract from this page.

		if (writable instanceof ScoredContent){
			int score;
			for (int j = 0; j < list.size(); j++){
				String[] split = ((String) list.get(j)).split(Constant.CONTENT_SEP);
				split[0].trim();
				score = Integer.parseInt(String.valueOf(split[0].charAt(0)));
				ScoredContent scoredContent = new ScoredContent(score,split[1]);
				records.add(new LamapacosWritable(scoredContent));
			}
			
		}else if(writable instanceof Text){
			for (int j = 0; j < list.size(); j++){
				Text text = new Text();
				text.set(list.get(j));
				records.add(new LamapacosWritable(text));
			}
		}
	}
}
