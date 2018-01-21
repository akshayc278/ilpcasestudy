package com.ilp;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
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

import com.ilp.UserMovVec.KeyMapper3;
public class CompVec extends Configured implements Tool{
	
	    public static Configuration conf;
	    public static Job job;
	    
	    
	    public static class KeyMapper4 extends Mapper<Text,Text,Text,Text>{
	    			private Text result = new Text();
	    			
	    			
	    			public static String newFunc1(String []x,String x2) throws FileNotFoundException{
	    				 String []x1=x[0].toString().split(",");
	    				 String []xx=x1[1].toString().split(" ");
	    				Scanner sc=new Scanner(new File ("/home/akshay/Desktop/newMovieTable"));
	    				String t1=null;
	    				while (sc.hasNextLine()){
	    					String str1[]=sc.nextLine().split("  ");
	    					String str2[]=str1[1].split(",");
	    					boolean flag=false;
	    					
	    					int count1=0;
	    					for(int i=0;i<x1.length;i++){
	    						
		    					if (Integer.parseInt(str1[0])==Integer.parseInt(xx[i]))
		    						flag=true;
		    				}
	    					//Object String;
							if(flag =true )continue;
							
							else {
								String ss[]= newFunct2(str2,x2);
								if (count1 < Integer.parseInt(ss[1]));
								{
									count1=Integer.parseInt(ss[1]);
									t1=str2[0];
								}
							}
	    						
	    					
	    				}
	    				String t=t1;
	    				
	    				return t;
	    				
	    			}
	    			
	    
	    
	          public static String [] newFunct2(String str2[],String x2){
	        	  int i1=Integer.parseInt(str2[1]);
	        	  int i2=Integer.parseInt(x2);
	        	  int arr1[]=new int[18];
	        	  int arr2[]=new int[18];

	        	  for(int k=0;k<18;k++){
	        		  int l=i1%10;
	        		  i1=i1/10;
	        		  arr1[k]=l;
	        		  
	        	  }
	        	  for(int k=0;k<18;k++){
	        		  int l=i2%10;
	        		  i1=i1/10;
	        		  arr2[k]=l;
	        		  
	        	  }
	        	  int count=0;
	        	  for(int p=0;p<18;p++){
	        		  if (arr1[p]==arr2[p])count++;
	        	  }
	        	  String st[]={str2[0],String.valueOf(count)};
	        	  return st;
	          }
	    			
	    			public void map(Text key, Text value,Context context) throws IOException, InterruptedException
	    		      {
	    				 
	    				 StringTokenizer itr = new StringTokenizer(value.toString());
	    				  String []k = itr.nextToken().split("		");
	    		 		 
	    		 		  key.set(k[0]);
	    		 		  String s=newFunc1(k,k[0]);
	    		 		  result.set(s);
	    		 		 context.write(key, result);
	    		      }
	    }
	    
	    
	    public int run (String[] args) throws Exception {
			
			conf = new Configuration();
			String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		    job = Job.getInstance(conf,"ilpproject");   
		  
		    //*** MAPPER CLASS ***//
		    Configuration KeyMapper2Config = new Configuration(false);
		    ChainMapper.addMapper(job, KeyMapper3.class, Text.class, Text.class, Text.class, Text.class, KeyMapper2Config);
		    job.setJarByClass(CompVec.class);
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
				int res = ToolRunner.run(conf, new CompVec(), args);
				System.exit(res);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
}
