package lab2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Lab2Reducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
	
		List<String> result = new ArrayList<>();
		Set<String> documents = new HashSet<>();
		
		// for each document add it to the term without duplicate
		for (Text value : values) {
			String dID = value.toString();
			if (!documents.contains(dID)) {
				result.add(dID);
				documents.add(dID);
			}
		}
		
		// consider the token only appear in one document as junk
		if (result.size() < 2) return;
		
		// sort the index
		Collections.sort(result);
		
		// output the result
		String resultStr = String.join(", ", result);
		context.write(new Text(key.toString() + " ->"), new Text(resultStr));
	}

}
