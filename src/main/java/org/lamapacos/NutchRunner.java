package org.lamapacos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.Crawl;
import org.apache.nutch.util.NutchConfiguration;

public class NutchRunner {
	public static void main(String[] args) throws Exception {
	    Configuration conf = NutchConfiguration.create();
	    int res = ToolRunner.run(conf, new Crawl(), args);
	    System.exit(res);
	}
}
