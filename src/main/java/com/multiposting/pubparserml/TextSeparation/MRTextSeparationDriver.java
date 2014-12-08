package com.multiposting.pubparserml.TextSeparation;

import com.google.common.base.Joiner;
import com.multiposting.pubparserml.clean.CleanMapper;
import com.multiposting.pubparserml.clean.CleanReducer;
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

import java.net.URI;

public class MRTextSeparationDriver extends Configured implements Tool {
    static public void main(String[] args) throws Exception {
        new MRTextSeparationDriver().run(args);
    }

    public int run(String[] args) throws Exception {
        System.out.println(Joiner.on("\n").join(args));
        Configuration conf = new Configuration();
        conf.set("filtername",args[3]);
        //conf.set("textinputformat.record.delimiter","@endline\n");

        Job job = Job.getInstance(conf);
        job.setJobName("pubparser-TextSeparation");
        job.setJarByClass(MRTextSeparationDriver.class);



        job.addCacheFile(new URI(args[2] + "#jsonlist"));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"_"+args[3]));

        job.setMapperClass(TextSeparationMapper.class);
        job.setReducerClass(TextSeparationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        return job.waitForCompletion(true)?0:1;
    }
}
