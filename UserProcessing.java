package com.ilp;
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

import com.ilp.Preprocessing.KeyMapper1;



public class UserProcessing extends Configured implements Tool{
	private static Configuration conf;
	private static Job job;

	
	public static class KeyMapper2 extends Mapper<Text,Text,Text,Text> {
		//private Text key =new Text ();
		private Text result = new Text();
		 int count=0;
		 public void map(Text key, Text value,Context context) throws IOException, InterruptedException
	      {
	         
	          if (count !=0){
	          StringTokenizer itr = new StringTokenizer(value.toString());
	 		  String []k = itr.nextToken().split(",");
	 		  key.set(k[0]+","+k[1]);
	          result.set(k[2]);
	          context.write(key, result);
	          count++;
	        }
	      }
		}
	
	public static class KeyReducer2 extends Reducer <Text,Text,Text,Text> {
		 
		private Text result = new Text();
    	private Text fkey = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
		   
			Double sum = 0.0;
            int  counter = 0;
            for (Text val : values)
            {
            Double dval = Double.valueOf(val.toString());
            if (dval != 0.0) {
            counter ++;
            sum += dval;
            }
            }
            Double average = sum/counter;
           
            result.set(" "+average.toString());
            fkey.set(key);
            context.write(fkey,result);
			   
			}
		   
		  
	
	   }
public int run (String[] args) throws Exception {
		
		conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
	    job = Job.getInstance(conf,"ilpproject");   
	  
	    //*** MAPPER CLASS ***//
	    Configuration KeyMapper2Config = new Configuration(false);
	    ChainMapper.addMapper(job, KeyMapper2.class, Text.class, Text.class, Text.class, Text.class, KeyMapper2Config);
	    job.setJarByClass(UserProcessing.class);
	    //job.setNumReduceTasks(0);

	    job.setReducerClass(KeyReducer2.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    return job.waitForCompletion(true) ? 0 : 1;
	    }

	public static void main(String[] args) {
		try {
			int res = ToolRunner.run(conf, new UserProcessing(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	}
