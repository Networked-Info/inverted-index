import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {

	public static void main(String[] args) throws IOException,
	ClassNotFoundException, InterruptedException {
		Path inp1 = new Path("lab2_data/wiki_00.csv");
		Path inp2 = new Path("lab2_data/wiki_01.csv");
		Path inp3 = new Path("lab2_data/wiki_02.csv");
		Path out = new Path("output");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "inverted index");
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, inp1);
		TextInputFormat.addInputPath(job, inp2);
		TextInputFormat.addInputPath(job, inp3);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, out);
		job.setJarByClass(Driver.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.waitForCompletion(true);
	}

}
