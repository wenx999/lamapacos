package org.lamapacos.util;

import java.io.File;

import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import jxl.read.biff.BiffException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public class DictionaryUtil extends Configured{
	
	public DictionaryUtil(){
		setConf(new Configuration());
	}	
	
	public DictionaryUtil(Configuration conf){
		setConf(conf);
	}
	
	/**
	 * 
	 * @param excelFile  the object of read excel  
	 */
	public HashMap<String, String[]> readExcel(File excelFile)throws BiffException, IOException{
		HashMap<String, String[]> hashmap = new HashMap<String, String[]>();
		Workbook rwb = null;
		Cell cell = null;
		
		rwb = Workbook.getWorkbook(excelFile);
		Sheet sheet = rwb.getSheet(0);
		for(int i = 1;  i < sheet.getRows(); i ++ ){
			String key = sheet.getCell(0, i).getContents();
			String[] value = new String[sheet.getColumns()-1];
			for (int j = 1; j < sheet.getColumns(); j ++){
				cell = sheet.getCell(j, i);
				value[j-1] = cell.getContents() == null ? new String("") : cell.getContents();
			}
			hashmap.put(key, value);
		}
		return hashmap;
	}
	
	/**
	 * 
	 * @param excelFile  the object of read excel  
	 */
	public boolean readExcel(File excelFile, Map<String, String[]> outSidemap){
		Workbook rwb = null;
		Cell cell = null;
		
		try {
			rwb = Workbook.getWorkbook(excelFile);
			Sheet sheet = rwb.getSheet(0);
			//get the content every row
			for(int i = 1;  i < sheet.getRows(); i ++ ){
				String key = sheet.getCell(0, i).getContents();
				String[] value = new String[sheet.getColumns()-1];
				for (int j = 1; j < sheet.getColumns(); j ++){
					cell = sheet.getCell(j, i);
					value[j-1] = cell.getContents() == null ? new String("") : cell.getContents();
				}
				outSidemap.put(key, value);
			}
		} catch (BiffException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	/**
	 * 
	 * read all the dictionary from the document which predefine in the lamapacos-preprocessor.xml 
	 */
	public HashMap<String, String[]> readDictionary (){
		HashMap<String, String[]> hashmap = new HashMap<String, String[]>();
		
		return (HashMap<String, String[]>) readDictionary(hashmap);
	}
	
	public Map<String, String[]> readDictionary (Map<String, String[]> outSideMap){
		int i;
		
		String regexs = getConf().get(Constant.SENTIMENT_DICTIONARY_REGEX, Constant.DEFAULT_SENTDIC_VALUE);
		File dir = new File(regexs);
		File[] dictionaryFile = dir.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				if(name.startsWith("."))
					return false;
				return true;
			}
		});
		for (i = 0; i < dictionaryFile.length; i ++){
			readExcel(dictionaryFile[i], outSideMap);
		}	
		return outSideMap;
	}
	
	/**
	 * read the degree dictionary which predifine in the lamapacos_preprocessor.aml
	 * @param outSideMap<String, String[]>
	 * @return outSideMap<String, String[]>
	 * @throws BiffException
	 * @throws IOException
	 */
	public Map<String, String[]> readDegreeDictionary () throws BiffException, IOException{
		Map<String, String[]> outSideMap;
		String regexs = getConf().get(Constant.DEGREE_DICTIONARY_REGEX, Constant.DEFAULT_DEGEDIC_VALUE);
		File excelFile = new File(regexs);
		outSideMap = readExcel(excelFile);
		return outSideMap;
	}
	
	public Map<String, String[]> readTagMapDictionary() throws IOException, BiffException{
		Map<String, String[]> outSideMap;
		String regex = getConf().get(Constant.TAGMAP_DICTIONARY_REGEX, Constant.DEFAULT_TAGMAPDIC_VALUE);
		File excelFile = new File(regex);
		outSideMap = readExcel(excelFile);
		return outSideMap;
	}
	
//	public static void main(String[] args){
//		String excelFileName = "dictionary.xls";
//			try {
//				HashMap<String, String[]> hashmap = DictionaryUtil.readExcel(new File(excelFileName));
//				Iterator iter = hashmap.entrySet().iterator();
//				while (iter.hasNext()) {
//				    Map.Entry entry = (Map.Entry) iter.next();
//				    Object key = entry.getKey();
//				   String[] val = (String[]) entry.getValue();
//				    System.out.println(key + " " + val[0] + " " + val[1]);
//				} 
//			} catch (BiffException e) {
//				e.printStackTrace();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//	}
}
