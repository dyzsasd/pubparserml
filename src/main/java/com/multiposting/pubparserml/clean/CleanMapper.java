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
        String res = data.replaceAll("\t","thisistab")
                .replaceAll("\\\\n"," ")
                .replaceAll("\\\\t"," ")
                .replaceAll("[^\\p{L}]+"," ")
                .replaceAll("thisistab","\t").toLowerCase();
        String[] items = res.split("\t");
        if(!(items[0].equals(" ")||items[0].equals("")||items[0]==null)
                &&(items[items.length-1].equals("societe descriptif")
                    || items[items.length-1].equals("description")
                    || items[items.length-1].equals("profil recherche"))){
            context.write(new Text(res),NullWritable.get());
        }

    }
}
