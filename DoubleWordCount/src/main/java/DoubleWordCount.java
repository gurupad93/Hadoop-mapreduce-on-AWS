package DoubleWordCountGroup.DoubleWordCountGroup;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//Main class
public class DoubleWordCount {
	public static void main(String [] args) throws Exception {
		//Parsing the Arguments
		Configuration conf=new Configuration();
		String[] files=new GenericOptionsParser(conf,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		
		//Configuring the job
		Job j= Job.getInstance(conf,"doublewordcount");
		j.setJarByClass(DoubleWordCount.class); //Main class
		j.setMapperClass(MapForWordCount.class);//Mapper class
		j.setReducerClass(ReduceForWordCount.class);//Reducer class
		
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(j, input);//Input directory
		FileOutputFormat.setOutputPath(j, output);//Output directory
		System.exit(j.waitForCompletion(true)?0:1); //Starting the job
	}
	
	//Mapper class for double word count
	public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String allLines = value.toString();// All lines from input files
			String[] allWords=allLines.split(" ");//All the words from input files
			/*For treating 2 words as one, isFirst maintained in the beginning.
			 *For first word it would be true which gets assigned to false.
			 * Next word will be combined with first word as isFirst is false.
			 * Second word will become first word
			 * Next will be second word and it is combined with first word from previous step
			 * These will repeat until the end.
			 */
			boolean isFirst = true;
			String firstWord = null;
			String secondWord = null;
			String combined = null;
			for(String word: allWords ) {
				if(isFirst) {
					firstWord = word;
					isFirst = false;
					continue;
				}
				secondWord = word;
				combined =firstWord+" " + secondWord;
				firstWord = secondWord;
				Text outputKey = new Text(combined.trim());
				IntWritable outputValue = new IntWritable(1);
				con.write(outputKey, outputValue);
			}
		}
	}
	//Reducer class for Double Word Count
	public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
			int sumOfValues = 0;
			//Aggregating the values
			for(IntWritable value : values) {
				sumOfValues += value.get();
			}
			con.write(word, new IntWritable(sumOfValues));
		}
	}


}
