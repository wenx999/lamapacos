package org.lamapacos.prepocessor.extraction;

import static org.junit.Assert.assertTrue;

import java.util.StringTokenizer;

import junit.framework.TestCase;

import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.lamapacos.preprocessor.extraction.HtmlExtractor;

//+        "<span class=\"sss s4\">34</span>"
//+        "<div> hao"
//+          "<p>haha</p>"
//+        "</div>"
public class TestHtmlExtractor{
	HtmlExtractor html= new HtmlExtractor();
	private static final String[] testPages ={
		new String("<html><head><title> title </title><script> script </script>"
	               + "</head><body> body <a href=\"http://www.nutch.org\">"
	               + " anchor </a><!--comment-->"
	               + "<div class=\"comment_list\" title=\"hao\">"
	               +   "<div class=\"details\">"
	               +     "<strong class=\"scoring\">"
	               +        "5分 非常满意"
	               +     "</strong>"
	               +     "<dd>"
	               +       "<strong class=\"merit\">内容: </strong>"
	               +       "不知所谓1111"
	               +     "</dd>"
	               +     "<dd class=\"merit\">123</dd>"
	               +     "<dd class=\"write clearfix\">456</dd>"
	               +   "</div>"
	               +   "<div class=\"details\">"
	               +     "<strong class=\"scoring\">"
	               +        "4分 非常满意"
	               +     "</strong>"
	               +     "<dd>"
	               +       "<strong class=\"merit\">内容: </strong>"
	               +       "不知所谓2222"
	               +     "</dd>"
	               +   "</div>"
	               + "</div>"
	               + "</body></html>"),
		new String("<html><head><title> title </title><script> script </script>"
	               + "</head><body> body <a href=\"http://www.nutch.org\">"
	               + " anchor </a><!--comment-->"
	               + "<div class=\"comment_list\">"
	               +   "<div class=\"details\">"
	               +     "<div class=\"time clearfix\">"
	               +        "<span>3</span>"
	               +        "<span class=\"sss s4\">34</span>"
	               +     "</div>"
	               +     "<div class=\"comment_content\">"
	               +       "<p>hao</p>"
	               +     "</div>"
	               +   "</div>"
	               +   "<div class=\"details\">"
	               +     "<div class=\"time clearfix\">"
	               +        "<span>3</span>"
	               +        "<span class=\"sss s4\">34</span>"
	               +     "</div>"
	               +     "<div class=\"comment_content\">"
	               +       "<p>hao</p>"
	               +     "</div>"
	               +   "</div>"
	               + "</div>"
	               + "</body></html>"),
		new String("<html><head><title> title </title><script> script </script>"
	               + "</head><body> body <a href=\"http://www.nutch.org\">"
	               + " anchor </a><!--comment-->"
	               + "<div class=\"comment_list\">"
	               +   "<div class=\"details\">"
	               +     "<div class=\"time clearfix\">"
	               +        "<span>3</span>"
	               +        "<span class=\"sss s4\">34</span>"
	               +     "</div>"
	               +     "<div class=\"comment_content\">"
	               +       "<p>hao</p>"
	               +     "</div>"
	               +   "</div>"
	               +   "<div class=\"details\">"
	               +     "<div class=\"time clearfix\">"
	               +        "<span>3</span>"
	               +        "<span class=\"sss s4\">34</span>"
	               +     "</div>"
	               +     "<div class=\"comment_content\">"
	               +       "<p>hao</p>"
	               +     "</div>"
	               +   "</div>"
	               + "</div>"
	               + "</body></html>")
	};
	private static final String[] answerText={
		new String("hso"),
		new String(),
		new String()
	};
	
	private static boolean equalsIgnoreWhitespace(String s1, String s2) {
	    StringTokenizer st1= new StringTokenizer(s1);
	    StringTokenizer st2= new StringTokenizer(s2);

	    while (st1.hasMoreTokens()) {
	      if (!st2.hasMoreTokens()) 
	        return false;
	      if ( ! st1.nextToken().equals(st2.nextToken()) )
	        return false;
	    }
	    if (st2.hasMoreTokens()) 
	      return false;
	    return true;
	  }
	@Test
	public void textExtractor(){
		int i;
		for (i= 0; i < testPages.length; i++) {		      
		      String text= html.extract(new Text(testPages[i])).toString();
		      assertTrue("expecting text: " + answerText[i] 
		                 + System.getProperty("line.separator") 
		                 + System.getProperty("line.separator") 
		                 + "got text: "+ text, 
		                 equalsIgnoreWhitespace(answerText[i], text));
		    }
		
	}
}
