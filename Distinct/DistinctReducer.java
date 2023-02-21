package com.DW;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistinctReducer extends Reducer <Text, IntWritable, Text,
NullWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Reducer<
Text, IntWritable, Text, NullWritable>.Context context)
          throws IOException , InterruptedException {
                int sum = 0;
                for (IntWritable value : values) {
                        sum += value.get();
                        if (sum == 1)
                                context.write(key, NullWritable.get());
                }
        }

}