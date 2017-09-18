package aws.emr.distributedwordcount;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.filecache.DistributedCache;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Set<String> importantWords = new HashSet<String>();
	private Text word = new Text();
	
	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try{
            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if(files != null && files.length > 0) {
                for(Path file : files) {
                    readFile(file);
                }
            }
        } catch(IOException ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }
    }


	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		System.out.println(key + ":" + value);

		String line = value.toString();
		String[] words = line.split("\\s+");
		for (int i = 0; i < words.length; i++ ) {
			if(importantWords.contains(words[i])) {
				word.set(words[i]);
				context.write(word, one);
			}
		}
	}
	
	private void readFile(Path filePath) {
        try{
            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
            String line = null;
            while((line = bufferedReader.readLine()) != null) {
            	String[] impwords = line.split("\\s+");
            	for(String impword : impwords) {
            		importantWords.add(impword.toLowerCase());
            	}
            }
            bufferedReader.close();
        } catch(IOException ex) {
            System.err.println("Exception in reading cache file: " + ex.getMessage());
        }
    }

}
