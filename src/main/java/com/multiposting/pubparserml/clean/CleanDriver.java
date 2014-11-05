package com.multiposting.pubparserml.clean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

class CleanDriver extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        conf.set("textinputformat.record.delimiter","@endline\n");

        Job job = Job.getInstance(conf);
        job.setJobName("pubparser-cleaning");

        job.setJarByClass(CleanDriver.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CleanMapper.class);
        job.setReducerClass(CleanReducer.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true)?0:1;
    }

    static public void main(String[] args){

    }
}