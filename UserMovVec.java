package com.ilp;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UserMovVec extends Configured implements Tool{
	private static Configuration conf;
	private static Job job;

	
	public static class KeyMapper3 extends Mapper<Text,Text,Text,Text> {
		private Text key =new Text ();
		private Text result = new Text();
		 
		 public void map(Text key, Text value,Context context) throws IOException, InterruptedException
	      {
	         
	         
	          StringTokenizer itr = new StringTokenizer(value.toString());
	 		  String []k = itr.nextToken().split(",");
	 		  String []k1=k[1].toString().split(" ");
	 		 int vec2[]=new int[k1.length];
	 		 Scanner inpfile = new Scanner(new File("/home/akshay/Desktop/moviestable"));
	 		
	      	while (inpfile.hasNextLine()) {
	      		
	      		
	      		String inp[]=inpfile.nextLine().toString().split("		");
	      		String inp1[]=inp[1].split(",");
	          
	         
	         
	 		  for(int l=0;l<k1.length;l++){
	 		      if (k1[l]==inp[0]){
	 		    	 int i1=Integer.parseInt(inp1[1]);
	 		    	 int arr1[]=new int[18];
	 		    	  for(int o=0;o<18;o++){
		        		  int t=i1%10;
		        		  i1=i1/10;
		        		  arr1[o]=t;
		        		  
		        	  }
	 		    	 for(int o=0;o<18;o++){
	 		    		vec2[o]=vec2[o]+arr1[o];
		        		  
		        	  }
	 		    	
	 		    }
	 		 }
	 		 
	 		 
	 		  for(int g=0;g<vec2.length;g++){
	 			  if (vec2[g]<Double.parseDouble(inp1[1])){
	 				  vec2[g]=0;
	 			  }
	 			  else
	 			  {vec2[g]=1;	 		
	 			  }
	 			  }
	 		  key.set(inp[0]);
	 		  inpfile.close();
	 		  result.set(vec2.toString());
	          context.write(key, result);
	         
	        }
	      }
		}
	
public int run (String[] args) throws Exception {
		
		conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
	    job = Job.getInstance(conf,"ilpproject");   
	  
	    //*** MAPPER CLASS ***//
	    Configuration KeyMapper2Config = new Configuration(false);
	    ChainMapper.addMapper(job, KeyMapper3.class, Text.class, Text.class, Text.class, Text.class, KeyMapper2Config);
	    job.setJarByClass(UserMovVec.class);
	    job.setNumReduceTasks(0);

	    //job.setReducerClass(KeyReducer3.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    return job.waitForCompletion(true) ? 0 : 1;
	    }

	public static void main(String[] args) {
		try {
			int res = ToolRunner.run(conf, new UserMovVec(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	}

	
     
