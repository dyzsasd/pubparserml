package com.multiposting.pubparserml.clean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by S-ZHANG on 04/11/2014.
 */
public class CleanReducer extends Reducer<LongWritable, Text, Text, NullWritable> {


}
