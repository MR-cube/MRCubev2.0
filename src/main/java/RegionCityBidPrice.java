import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RegionCityBidPrice implements Tool{

	Configuration conf = null;
	
  public static class MRCubeMapper
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    @Override
	/**
	 * map method that performs the tokenizer job and framing the initial key value pairs
	 * @param  key  is a long integer offset.
	 * @param  value is a line of text.
	 * @param  context is an instance of Context to write output to.
	 **/
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    
    	String line = value.toString();
    	String[] RegionCity = line.split(","); 	
    	try {
	    	if ((Integer.parseInt(RegionCity[7]) < 1000) & 
	    			(Integer.parseInt(RegionCity[8]) < 1000) & 
	    			(Integer.parseInt(RegionCity[20]) < 1000)) {
	    		
	    		context.write(new Text( RegionCity[7] + "  " + RegionCity[8]), 
	    				new IntWritable(Integer.parseInt(RegionCity[20])));
	    	}
    	} catch(Exception e) {
    		// Ignore exception
    	}
    }
  }

  public static class MRCubeReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    
	private IntWritable result = new IntWritable();
	
	@Override
	/**
	 *  reduce method accepts the Key Value pairs from mappers, 
	 *  do the aggregation based on keys and produce the final out put.
	 *  @param  key, values, context
	 **/
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0, count = 0;
      for (IntWritable val : values) {
        sum += val.get();
        count ++;
      }
      result.set(sum/count);
      context.write(key, result);
    }
  }

	  public static void main(String[] args) throws Exception {
		  
		  int result = ToolRunner.run(new RegionCityBidPrice(), args);
		  System.exit(result);
		  
		  
	    Configuration conf = new Configuration();
	    Job job = new Job();
	    job.setJobName("MRCube");
	    job.setJarByClass(RegionCityBidPrice.class);
	    job.setMapperClass(MRCubeMapper.class);
	    job.setCombinerClass(MRCubeReducer.class);
	    job.setReducerClass(MRCubeReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	
	public int run(String[] args) throws Exception {
		// Check for valid number of arguments.
		if (args.length < 1) {
			System.err.println("*** Error: Missing Parameters *** \n " +
									   "Usage: hadoop Homerun <output_path>");
			System.exit(-1);
		}
		
		Configuration conf = getConf();
		 
		/**
		 * Create a new job object and set the output types of the Map and Reduce function.
		 * Also set Mapper and Reducer classes.
		 */
		Job job = new Job(conf, "MRCube");
		job.setJarByClass(RegionCityBidPrice.class);
		job.setMapperClass(MRCubeMapper.class);
		job.setCombinerClass(MRCubeReducer.class);
		job.setReducerClass(MRCubeReducer.class);
		 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		 
		// the HDFS input and output directory to be fetched from the command line
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
	    return (job.waitForCompletion(true) ? 0 : 1); 
	}

	public Configuration getConf() {
		conf = new Configuration();
		return conf;
	}

	public void setConf(Configuration arg0) {}
}