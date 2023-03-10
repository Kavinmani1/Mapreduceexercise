package com.oddeven;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
public class OddEvenMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{
	public void map(LongWritable key,Text value,OutputCollector<Text,IntWritable>output, Reporter reporter)
	throws IOException {
		String[]data =value.toString().split(",");
		for(String num :data)
		{
			int number =Integer.parseInt(num); //parse string returns int
			if(number %2 ==0)
			{
				output.collect(new Text("EVEN"),new IntWritable(number));
			}else
				output.collect(new Text("ODD"),new IntWritable(number));
			}
		}
	}





























			
