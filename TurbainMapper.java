package com.Turbain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TurbainMapper extends Mapper<LongWritable, Text, String, IntWritable>
{


	  public void map(LongWritable key, Iterator<Text> values, Context context)
	      throws IOException, InterruptedException {
		  //ArrayList<String[]> turbaine =new ArrayList<String[]>();
	    	 //while (values.hasNextLine()) {
	    		  //Text value = values.nextLine();
	      String arr[]=values.toString().split(",");
	      if(arr[4]=="-1")
	    		  {
	    			  arr[3]="null";
	    		  }
	    		 // turbaine.add(arr);
	    	 // }
	    	  context.write(new String(arr[]), new IntWritable(1));
	  }
}


	    

