package com.DW;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DistinctMapper extends Mapper <LongWritable, Text, Text,
IntWritable>{
        public static final IntWritable one = new IntWritable(1);



        public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
           throws IOException , InterruptedException {

                String[] words = value.toString().split(" ");
                for (String string : words) {
                context.write(new Text(string.replaceAll("[-+.^:,?]","")), one);
                }
        }


}



