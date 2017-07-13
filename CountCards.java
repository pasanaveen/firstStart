package drivers;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

import mappers.CountMapper;
import reducers.CountReducer_NoKeyRecord;

public class CountCards extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new CountCards(), args);
		System.exit(exitCode);
	}

	public int run(String[] arg0) throws Exception {
		// Create the job object. And get the instance of configuration 
		Job job = Job.getInstance(getConf(), "Row count using built in mappers and reducers");
		
		//Now set the jar we want to run to the job object. We set jar by class name
		job.setJarByClass(getClass());
		
		// Now set the input format of the file
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		//job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(CountMapper.class);
		
		// now set the output keys and values
		// set output key class from mapper. Set output value class from mapper.
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// now write the custom reducer
		
		job.setReducerClass(CountReducer_NoKeyRecord.class);
		
		//now set the output key class, and output value class for reducer
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileOutputFormat.setOutputName(job, new Path(args[1]));
		
		

		
		
		return 0;
	}

}
