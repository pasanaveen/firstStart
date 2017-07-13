package reducers;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

public class CountReducer_NoKeyRecord extends Reducer<Text, IntWritable, NullWritable, IntWritable>{

	@Override
	public void reduce(Text key, Iterable<IntWritable> records, Context context)
			throws IOException, InterruptedException{
		
		int sum = 0;
		
		for (IntWritable record : records){
			sum += record.get();
		}
		
	}
}
