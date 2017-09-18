package aws.emr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		System.out.println(key + ":" + value);

		String line = value.toString();
		String[] words = line.split("\\s+");
		for (int i = 0; i < words.length - 1; i++ ) {
			word.set(words[i] + "," + words[i+1]);
			context.write(word, one);
		}
	}
}
