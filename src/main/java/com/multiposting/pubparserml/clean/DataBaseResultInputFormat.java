package com.multiposting.pubparserml.clean;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class DataBaseResultInputFormat extends FileInputFormat<LongWritable, Text> implements JobConfigurable {

    private CompressionCodecFactory compressionCodecs = null;

    public void configure(JobConf conf) {
        compressionCodecs = new CompressionCodecFactory(conf);
    }

    protected boolean isSplitable(FileSystem fs, Path file) {
        return compressionCodecs.getCodec(file) == null;
    }

    static class EmptyRecordReader implements RecordReader<LongWritable, Text> {
        @Override
        public void close() throws IOException {}
        @Override
        public LongWritable createKey() {
            return new LongWritable();
        }
        @Override
        public Text createValue() {
            return new Text();
        }
        @Override
        public long getPos() throws IOException {
            return 0;
        }
        @Override
        public float getProgress() throws IOException {
            return 0;
        }
        @Override
        public boolean next(LongWritable key, Text value) throws IOException {
            return false;
        }
    }

    public RecordReader<LongWritable, Text> getRecordReader(
            InputSplit genericSplit, JobConf job,
            Reporter reporter)
            throws IOException {

        reporter.setStatus(genericSplit.toString());

        // This change is required for CombineFileInputFormat to work with .gz files.

        // Check if we should throw away this split
        long start = ((FileSplit)genericSplit).getStart();
        Path file = ((FileSplit)genericSplit).getPath();
        final CompressionCodec codec = compressionCodecs.getCodec(file);
        if (codec != null && start != 0) {
            // (codec != null) means the file is not splittable.

            // In that case, start should be 0, otherwise this is an extra split created
            // by CombineFileInputFormat. We should ignore all such extra splits.
            //
            // Note that for the first split where start = 0, LineRecordReader will
            // ignore the end pos and read the whole file.
            return new EmptyRecordReader();
        }

        String delimiter = job.get("textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter) {
            recordDelimiterBytes = delimiter.getBytes();
        }
        return new LineRecordReader(job, (FileSplit) genericSplit,
                recordDelimiterBytes);
    }
}
