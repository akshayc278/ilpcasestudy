


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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





public class ReverseTrackMName extends Configured implements Tool{
	private static Configuration conf;
	private static Job job;
	
	
	public static class KeyMapper10 extends Mapper<Text,Text,Text,Text> {
		private Text key =new Text ();
		private Text result = new Text();
		
		
		 public void map(Text key, Text value,Context context) throws IOException, InterruptedException
	      { 
	            
			 String mkey=key.toString().trim();
			 String k[]=value.toString().split(",");
			 Scanner inpfile1 = new Scanner(new File("/home/akshay/Desktop/movievector"));
			
			 String movieid=null;
			// String moviename=null;
			 
			 while(inpfile1.hasNextLine()){
				 String ln[]=inpfile1.nextLine().split("\\s+");
				 if(k[1].equals((ln[1].toString().trim()))){
				       
				           movieid=ln[0].toString();
				  	
				           		
				           		 
				           	
				           	
					 
						 }
						 
					 }
			 		inpfile1.close();
			 
		     key.set(mkey);
             result.set(movieid);
             context.write(key, result);
		 	
	      }
 	   }
	
public int run (String[] args) throws Exception {
		
		conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
	    job = Job.getInstance(conf,"ilptask10");   
	  
	    //*** MAPPER CLASS ***//
	    Configuration KeyMapper10Config = new Configuration(false);
	    ChainMapper.addMapper(job, KeyMapper10.class, Text.class, Text.class, Text.class, Text.class, KeyMapper10Config);
	    job.setJarByClass(ReverseTrackMName.class);
	     job.setNumReduceTasks(0);

	   //job.setReducerClass(KeyReducer2.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    return job.waitForCompletion(true) ? 0 : 1;
	    }

	public static void main(String[] args) {
		try {
			int res = ToolRunner.run(conf, new ReverseTrackMName(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	}

