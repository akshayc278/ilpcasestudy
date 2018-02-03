
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.chain.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;






public class Vector1 extends Configured implements Tool {
	private static Configuration conf;
	private static Job job;
  
	

          
 	
	public static class KeyMapper2 extends Mapper<Text,Text,Text,Text> {
		private Text key =new Text ();
		private Text result = new Text();
		 int count=0;
		
	
		
		 public void map(Text key, Text value, Context context) throws IOException {
		     
			 			key.set(key);
		            	String k[]=value.toString().split(",");
		            	
		                  result.set(k[1]);
		                  
							try {
								context.write(key, result);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						
		                 
		           
		   }      
		       
		}

	          
	 	
	
	
	
	public int run (String[] args) throws Exception {
		
		conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
	    job = Job.getInstance(conf,"ilpproject");   
	  
	   

	    //*** FIRST MAPPER CLASS ***//
	    Configuration KeyMapper01Config = new Configuration(false);
	    ChainMapper.addMapper(job, KeyMapper2.class, Text.class, Text.class, Text.class, Text.class, KeyMapper01Config);
	    job.setJarByClass(Vector1.class);
	    job.setNumReduceTasks(0);

	    
	    //job.setReducerClass();
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    return job.waitForCompletion(true) ? 0 : 1;
	    }

	public static void main(String[] args) {
		try {
			int res = ToolRunner.run(conf, new Vector1(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
