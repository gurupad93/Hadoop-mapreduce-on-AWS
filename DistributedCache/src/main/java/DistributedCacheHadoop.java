package DistributedCache.DistributedCache;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
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
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

//Main class
public class DistributedCacheHadoop {

	public static void main(String [] args) throws Exception {
			//Command line Arguments 
			Configuration conf=new Configuration();
			String[] files=new GenericOptionsParser(conf,args).getRemainingArgs();
			Path input=new Path(files[0]);
			Path output=new Path(files[1]);
			String s3DistributedCacheFile = files[2];
			
			//Job Configuration
			Job j= Job.getInstance(conf,"DistributedCache");
			j.setJarByClass(DistributedCacheHadoop.class); //Configure Main Class
			j.setMapperClass(DistributedCacheMapper.class);//Configure Mapper Class
			j.setReducerClass(DistributedCacheReducer.class);//Configure Reducer Class
			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(IntWritable.class);
			//Adding the distributed Cache file..File can be accessed locally using name patternsfile
			j.addCacheFile(new URI(s3DistributedCacheFile+"#patternsfile"));
			
			//Input and output directories
			FileInputFormat.addInputPath(j, input);
			FileOutputFormat.setOutputPath(j, output);
			
			//Starting the Job
			System.exit(j.waitForCompletion(true)?0:1);
		}
	
	
		//Mapper class for Distributed Cache
		public static class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
			public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
					String line = value.toString();
					String[] words=line.split(" "); //Array of words from input files
					String contentsOfPatternsFile=FileUtils.readFileToString(new File("./patternsfile"));//Contents of Distributed cache file
					String[] wordsFromPatternsFile=contentsOfPatternsFile.split(" ");
					Set<String> uniqueWords = new HashSet<String>(Arrays.asList(wordsFromPatternsFile));//Unique words from the patterns file
					for(String word: words ) {
							/*See if the word from input file is present in distributed cache file
							 *If yes, map it else next 
							 */
							if(uniqueWords.contains(word.trim())) {
								Text wordAsKey = new Text(word.trim());
								IntWritable wordValue = new IntWritable(1);
								con.write(wordAsKey, wordValue);
							}
						}
				}
		}
	
		
		//Reducer class for Distributed cache
		public static class DistributedCacheReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
				public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
						int sumOfValues = 0;
						//Aggregate the values of each word
						for(IntWritable val : values) {
								sumOfValues += val.get();
							}
						con.write(word, new IntWritable(sumOfValues));
					}
			}


}
