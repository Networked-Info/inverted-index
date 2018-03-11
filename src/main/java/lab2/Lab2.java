package lab2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Lab2 {
	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		
		Path inp_00 = new Path(args[0]);
		Path inp_01 = new Path(args[1]);
		Path inp_02 = new Path(args[2]);
		Path out = new Path(args[3]);
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "lab2 inverted index");
		
		// load wiki_00.csv
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, inp_00);
		// load wiki_01.csv
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, inp_01);
		// load wiki_02.csv
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, inp_02);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, out);
		
		job.setJarByClass(Lab2.class);
		job.setMapperClass(Lab2Mapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(Lab2Reducer.class);
		
		job.waitForCompletion(true);
	}

}
