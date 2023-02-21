package com.Weather;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class WeatherReducer extends
		Reducer<Text, Text, Text, Text> {

	/**
	* @method reduce
	* This method takes the input as key and list of values pair from mapper, it does aggregation
	* based on keys and produces the final context.
	*/
	
	public void reduce(Text Key, Iterator<Text> Values, Context context)
			throws IOException, InterruptedException {

		
		//putting all the values in temperature variable of type String
		
		String temperature = Values.next().toString();
		context.write(Key, new Text(temperature));
	}

}

