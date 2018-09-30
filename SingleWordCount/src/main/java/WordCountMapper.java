package WordCountGroup.WordCountArtifact;

import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
/**
* Mapper class for Distributed Cache. 
* Maps each word in input files.
*/
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
		//Get All lines from the input files
		String allLines = value.toString();
		//Extract all the words
		String[] allWords=allLines.split(" ");
		//Map each of the extracted words.
		for(String word: allWords ) {
			Text outputKeyWord = new Text(word.trim());
			IntWritable outputValue = new IntWritable(1);
			con.write(outputKeyWord, outputValue);
		}
	}
}
