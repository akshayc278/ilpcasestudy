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






public class Preprocessing extends Configured implements Tool {
	private static Configuration conf;
	private static Job job;

	
	
	public static class KeyMapper1 extends Mapper<Text,Text,Text,Text> {
	private Text key =new Text ();
	private Text result = new Text();
	 int count=0;
	
	public static int[] vectr(String str){
		int vec[]=new int [18];
		String []str1={"unknown","Action","Adventure","Animation","Children's","Comedy","Crime","Documentory","Drama","Fantacy","File-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thriller","War"};
		for(int i=0;i<str1.length;i++){
			if (vec[i]==1) continue;
			if (str1[i]==str){
				vec[i]=1 ;
			}
			else 
				vec[i]=0;
		}
	
		return vec;
	}
	
	  public void map(Text key, Text value,Context context) throws IOException, InterruptedException
      {
         
          if (count !=0){
          StringTokenizer itr = new StringTokenizer(value.toString());
 		  String []k = itr.nextToken().split(",");
 		 String s="k[1]";
 		  String []k1=k[2].toString().split("|");
 		  int rvec[]=new int[18];
 		  for(int j=0;j<k1.length;j++){
 			  rvec=vectr(k1[j]);
 		  }
 		  key.set(k[0]);
          result.set(s+","+rvec);
          context.write(key, result);
          count++;
        }
      }
	}
	public int run (String[] args) throws Exception {
		
		conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
	    job = Job.getInstance(conf,"ilpproject");   
	  
	    //*** FIRST MAPPER CLASS ***//
	    Configuration KeyMapper1Config = new Configuration(false);
	    ChainMapper.addMapper(job, KeyMapper1.class, Text.class, Text.class, Text.class, Text.class, KeyMapper1Config);
	    job.setJarByClass(Preprocessing.class);
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
			int res = ToolRunner.run(conf, new Preprocessing(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}