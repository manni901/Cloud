package aws.emr.distributedwordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;

public class WordCount {

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(WordCount.class);
		job.setJobName("doublewordcount");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		DistributedCache.addCacheFile(new Path(args[11]).toUri(), job.getConfiguration());

		Path[] inputPaths = new Path[10];
		for(int i = 0; i < 10; i++)
		{
			inputPaths[i] = new Path(args[i]);
		}
		
		FileInputFormat.setInputPaths(job, inputPaths);
		FileOutputFormat.setOutputPath(job, new Path(args[10]));

		job.waitForCompletion(true);
	}
}
