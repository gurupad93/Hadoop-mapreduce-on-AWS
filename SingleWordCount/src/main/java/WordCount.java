package WordCountGroup.WordCountArtifact;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//Main class
public class WordCount extends Configured implements Tool {
	//Main function which calls run function of ToolRunner to configure job
	public static void main(String[] args) throws Exception{
		int ec = ToolRunner.run(new WordCount(), args);
		System.exit(ec);
	}
 
	//run function
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s <general options> <i/p> <o/p>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
	
		//Configure Job
		Job job = new org.apache.hadoop.mapreduce.Job();
		job.setJarByClass(WordCount.class); //Main class
		job.setJobName("WordCounter");
		
		FileInputFormat.addInputPath(job, new Path(args[0])); //Input files
		FileOutputFormat.setOutputPath(job, new Path(args[1]));//Output
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(WordCountMapper.class); //Mapper class
		job.setReducerClass(WordCountReducer.class);//Reducer class
	
		int returnValue = job.waitForCompletion(true) ? 0:1; //start the job
		return returnValue;
	}
}
