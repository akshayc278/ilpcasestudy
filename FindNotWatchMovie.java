

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





public class FindNotWatchMovie extends Configured implements Tool{
	private static Configuration conf;
	private static Job job;
	
	
	public static class KeyMapper6 extends Mapper<Text,Text,Text,Text> {
		private Text key =new Text ();
		private Text result = new Text();
		
		
		 public void map(Text key, Text value,Context context) throws IOException, InterruptedException
	      { 
	            
		
			 String k[]=value.toString().split(",");
			 Scanner inpfile = new Scanner(new File("/home/akshay/Desktop/movievector"));
			 String stt=null;
			 boolean flag=false;
			 while(inpfile.hasNextLine()){
				 String ln[]=inpfile.nextLine().split("\\s+");
				 flag=false;
				 
				 for(int i=0;i<k.length;i++){
					 if(Integer.parseInt(ln[0])==Integer.parseInt(k[i])){
						 flag=true;
						 break;
					 }
					 
				 }
					 if(flag==false)
				 {
					 if(stt==null){
						 stt=ln[1];
					 }
					 else
					 {
						 stt=stt.concat(","+ln[1]);
					 }
					 
				 }
			 }
			 
		     key.set(key);
             result.set(stt);
             context.write(key, result);
		 	
	      }
 	   }
	
public int run (String[] args) throws Exception {
		
		conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
	    job = Job.getInstance(conf,"ilptask07");   
	  
	    //*** MAPPER CLASS ***//
	    Configuration KeyMapper6Config = new Configuration(false);
	    ChainMapper.addMapper(job, KeyMapper6.class, Text.class, Text.class, Text.class, Text.class, KeyMapper6Config);
	    job.setJarByClass(FindNotWatchMovie.class);
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
			int res = ToolRunner.run(conf, new FindNotWatchMovie(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	}
