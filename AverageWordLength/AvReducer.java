package com.AverageWordLength;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvReducer extends
Reducer<Text, IntWritable, Text, DoubleWritable> {

/**
* The reduce method runs once for each key received from
* the shuffle and sort phase of the MapReduce framework.
* The method receives:
* @param A key of type Text
* @param A set of values of type IntWritable
* @param A Context object
*/
@Override
public void reduce(Text key, Iterable<IntWritable> values, Context context)
  throws IOException, InterruptedException {

double sum = 0, count = 0;

/*
 * For each value in the set of values passed to us by the mapper:
 */
for (IntWritable value : values) {
  
  /*
   * Add up the values and increment the count
   */
  sum += value.get();
  count++;
}
if (count != 0d) {
  
  /*
   * The average length is the sum of the values divided by the count.
   */
  double result = sum / count;
 
  /*
   * Call the write method on the Context object to emit a key
   * (the words' starting letter) and a value (the average length 
   * per word starting with this letter) from the reduce method. 
   */
  context.write(key, new DoubleWritable(result));
}
}
}

