package lab2;

import org.apache.hadoop.mapreduce.Mapper;

import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.stemmer.snowball.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import lab2.StopWords;


public class Lab2Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Tokenizer tokenizer = SimpleTokenizer.INSTANCE;
	private Set<String> stopWords = StopWords.STOPWORDS;
	private SnowballStemmer stemmer = new SnowballStemmer(SnowballStemmer.ALGORITHM.ENGLISH);
	
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		// parse the line with ","
		String[] list = value.toString().split(",");
		
		// parse the lien into categories
		if (list.length > 1) {
			String docID = list[0];
//			String docURL = list[1]; // not generate terms from docURL
			String docTitle = list[2];
			String docContent = String.join("", Arrays.copyOfRange(list, 3, list.length));
			
			generateInvertIndex(docID, docTitle, context);
			generateInvertIndex(docID, docContent, context);
		}
	}
	
	void generateInvertIndex(String id, String content, Context context) throws IOException, InterruptedException {
		// lower the case
		content = content.toLowerCase();
		
		// tokenize content
		String[] tokens = tokenizer.tokenize(content);
		
		for (String token : tokens) {
			// remove punctuation
			if (token.matches("\\W+")) { continue; }
			// remove underbar
			if (token.matches("\\_+")) { continue; } 
			// remove non zipcode numbers
			if (token.matches("\\d+") && token.length() != 5) { continue; }
			// remove stop words
			if (stopWords.contains(token)) { continue; }
			// remove one character token
			if (token.length() < 2) { continue; }
			// stem the word with Snowball algorithm
			String stemmedToken = String.valueOf(stemmer.stem(token));
			if (stemmedToken.length() > 2) {
				token = stemmedToken;
			}
		
			// output the result of mapper
			context.write(new Text(token), new Text(id));	
		}
		
	}
	
}
