import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.en.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;


public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] row = value.toString().split(",");
		String content = row[3];
		content = content.replaceAll("\\d","").replaceAll("[^\\w\\s\\-]", " ");
		Analyzer analyzer = new StandardAnalyzer();
	    TokenStream result = analyzer.tokenStream(null, content);
	    result = new PorterStemFilter(result);
	    result = new StopFilter(result, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
	    CharTermAttribute resultAttr = result.addAttribute(CharTermAttribute.class);
	    result.reset();

	    List<String> tokens = new ArrayList<>();
	    while (result.incrementToken()) {
	        tokens.add(resultAttr.toString());
	    }
	    analyzer.close();
	    result.close();
	    for (String t: tokens) {
	    	context.write(new Text(t), new Text(row[0]));
	    }
	}
	
}
