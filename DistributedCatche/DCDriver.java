package com.DC;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DCDriver extends Configured implements Tool{
	public int run(String[] args) throws Exception {
	      JobConf conf = new JobConf(getConf(), DCDriver.class); 
	      conf.setJobName("wordcount");
	      conf.setOutputKeyClass(Text.class);
	      conf.setOutputValueClass(IntWritable.class);
	      conf.setMapperClass(DCMapper.class);
	      conf.setCombinerClass(DCReducer.class);
	      conf.setReducerClass(DCReducer.class);
	      conf.setInputFormat(TextInputFormat.class);
	      conf.setOutputFormat(TextOutputFormat.class);
	      List<String> other_args = new ArrayList<String>(); 

	      for (int i=0; i < args.length; ++i) {
	        if ("-skip".equals(args[i])) {
	          DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf); 
	          conf.setBoolean("wordcount.skip.patterns", true); 
	        } else {

	          other_args.add(args[i]);
	        }
	      }
	      FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
	      FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
	      JobClient.runJob(conf);
	      return 0;
	    }
	    public static void main(String[] args) throws Exception { 

	      int res = ToolRunner.run(new Configuration(), new DCDriver(), args); 
	      System.exit(res);
	    }
	} 


