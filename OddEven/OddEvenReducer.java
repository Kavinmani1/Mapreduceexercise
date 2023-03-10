package com.oddeven;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class OddEvenReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,String>{
	public void reduce(Text key,Iterator<IntWritable> values,OutputCollector<Text,String> output,Reporter reporter)
	throws IOException{
		String s =" ";
		while(values.hasNext()) //check if scanner as another token in its input
		{
			IntWritable temp=values.next();
			s += temp + " ";
		}
		output.collect(key, new String(s));
		}
}


