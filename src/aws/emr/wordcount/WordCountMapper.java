package aws.emr.wordcount;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WordCountMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		//2014–07–05 20:00:00 LHR3 4260 10.0.0.15 GET eabcd12345678.cloudfront.net /test-image–1.jpeg 200 - Mozilla/5.0%20(MacOS;%20U;%20Windows%20NT%205.1;%20en-US;%20rv:1.9.0.9)%20Gecko/2009040821%20IE/3.0.9
		 String regEx = "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} \\d{4} \\d{2}.\\d{1}.\\{1}.\\{d{2} \\s{3} \\s{5}d{8}.\\s{10}.\\s{3} \\/s{4}-\\s{5}-\\d{1}.\\s{4} \\d{3}";
		while (tokenizer.hasMoreTokens())
		{
			word.set(tokenizer.nextToken());
			String t = word.toString();
			
			if(t.matches(regEx))
			{
			output.collect(word, one);
			}
		}
	}
}