package WordCountGroup.WordCountArtifact;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Reducer class
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context c) throws IOException, InterruptedException {
		int sumOfValues = 0;
		Iterator<IntWritable> valuesIt = values.iterator();
		//Aggregate the values
		while(valuesIt.hasNext()){
			sumOfValues = sumOfValues + valuesIt.next().get();
		}
		c.write(key, new IntWritable(sumOfValues));
	}	
}
