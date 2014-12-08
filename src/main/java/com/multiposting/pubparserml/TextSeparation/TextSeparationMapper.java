package com.multiposting.pubparserml.TextSeparation;


import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.List;

public class TextSeparationMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String filername;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] fileUris;
        List<String> jsonProfiles = Lists.newArrayList();
        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {
            fileUris = context.getCacheFiles();

            for(URI fileUri:fileUris){
                BufferedReader br = new BufferedReader(new FileReader("./jsonlist"));
                String line = "";
                while((line=br.readLine())!=null){
                    jsonProfiles.add(line);
                }
            }
        }

        filername = context.getConfiguration().get("filtername","fr");

        try {
            DetectorFactory.loadProfile(jsonProfiles);
        } catch (LangDetectException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Detector detector = null;
        try {
            detector = DetectorFactory.create();
            detector.append(value.toString());
            if(filername.equals(detector.detect())){
                context.write(new Text(detector.detect()),value);
            }
        } catch (LangDetectException e) {
            context.write(new Text("DetectorException"),value);
            e.printStackTrace();
        }
    }
}
