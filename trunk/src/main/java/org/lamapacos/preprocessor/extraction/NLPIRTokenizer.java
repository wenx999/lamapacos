/**
 * 
 */
package org.lamapacos.preprocessor.extraction;

import kevin.zhang.NLPIR;


/**
 * @author hadoop
 *
 */
public class NLPIRTokenizer implements Tokenizer{
	public NLPIRTokenizer() {}
	
	public String segment(String source) {
		try {
			NLPIR testNLPIR = new NLPIR();

			String argu = "";
//			System.out.println("NLPIR_Init");
			if (testNLPIR.NLPIR_Init(argu.getBytes("UTF-8"), 1) == false) {
				System.out.println("Init Fail!");
				return null;
			}
			// 导入用户词典前
			byte nativeBytes[] = testNLPIR.NLPIR_ParagraphProcess(source.getBytes("UTF-8"), 1);
			String nativeStr = new String(nativeBytes, 0, nativeBytes.length, "UTF-8");

//			System.out.println("分词结果为： " + nativeStr);
			testNLPIR.NLPIR_Exit();
			return new String(nativeBytes);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return null;
	}
}
