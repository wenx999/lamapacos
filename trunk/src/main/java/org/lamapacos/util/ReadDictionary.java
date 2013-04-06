package org.lamapacos.util;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
//import java.util.Iterator;
//import java.util.Map;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import jxl.read.biff.BiffException;
public class ReadDictionary {
	private Configuration conf;
	private static final String DICTIONARY_REGEX = "dir.dictionary.location";
	
	public ReadDictionary(){
		conf = new Configuration();
		this.addResource();
	}	
	public ReadDictionary(Configuration conf){
		this.conf = conf;
		this.addResource();
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
				value[j-1] = cell.getContents();
			}
			hashmap.put(key, value);
		}
		return hashmap;
	}
	
	/**
	 * 
	 * @param excelFile  the object of read excel  
	 */
	public boolean readExcel(File excelFile, HashMap<String, String[]> hashmap){
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
					value[j-1] = cell.getContents();
				}
				hashmap.put(key, value);
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
	public HashMap<String, String[]> readDictionaryDir (){
		int i;
		HashMap<String, String[]> hashmap = new HashMap<String, String[]>();
		
		String regexs = conf.get(DICTIONARY_REGEX, "dictionary");
		File dir = new File(regexs);
		File[] dictionaryFile = dir.listFiles();
		for (i = 0; i < dictionaryFile.length; i ++){
			readExcel(dictionaryFile[i], hashmap);
		}
		
		return hashmap;
		
	}
//	public static void main(String[] args){
//		String excelFileName = "dictionary.xls";
//			try {
//				HashMap<String, String[]> hashmap = ReadDictionary.readExcel(new File(excelFileName));
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