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
import java.util.ArrayList; 
import java.util.Collections; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class NameFinder extends Configured implements Tool {
	private static Configuration conf;
	private static Job job;
	public static class KeyMapper11 extends Mapper<Text,Text,Text,Text> {
		private Text key =new Text ();
		private Text result = new Text();
	  
		
		 public void map(Text key, Text value,Context context) throws IOException, InterruptedException
		  { 
	            
			 String lkey=key.toString().trim();
			 String k=value.toString();
			 Scanner inpfile2 = new Scanner(new File("/home/akshay/Desktop/movierawtext"));
			
			 //String movieid=null;
			String moviename=null;
			 
			 while(inpfile2.hasNextLine()){
				 String ln[]=inpfile2.nextLine().split("\\t");
				 String lll[]=ln[1].split(",");
			
				 if(k.equals((ln[0].toString().trim()))){
				       		
				           		 
				           moviename=lll[0];	
				           	
					 
						 }
						 
					 }
			 inpfile2.close();
			 
		     key.set(lkey);
             result.set(moviename);
             context.write(key,result);
		  }
		  }
	      
 	   
	public int run (String[] args) throws Exception {
		
		conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
	    job = Job.getInstance(conf,"ilptask11");   
	  
	    //*** MAPPER CLASS ***//
	    Configuration KeyMapper10Config = new Configuration(false);
	    ChainMapper.addMapper(job, KeyMapper11.class, Text.class, Text.class, Text.class, Text.class, KeyMapper10Config);
	    job.setJarByClass(NameFinder.class);
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
			int res = ToolRunner.run(conf, new NameFinder(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

