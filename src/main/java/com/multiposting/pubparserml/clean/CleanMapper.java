package com.multiposting.pubparserml.clean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by S-ZHANG on 04/11/2014.
 */
public class CleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        data = data.replaceAll("\t","@tab");
        data = data.replaceAll("\\\\n"," ");
        data = data.replace("[^\\p{L}]+"," ");
        data = data.replaceAll("@tab","\t");
        context.write(new Text(data),NullWritable.get());

    }
}
