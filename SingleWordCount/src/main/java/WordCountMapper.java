package WordCountGroup.WordCountArtifact;

import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
//Mapper class
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
		String allLines = value.toString();
		String[] allWords=allLines.split(" ");
		for(String word: allWords ) {
			Text outputKeyWord = new Text(word.trim());
			IntWritable outputValue = new IntWritable(1);
			con.write(outputKeyWord, outputValue);
		}
	}
}
