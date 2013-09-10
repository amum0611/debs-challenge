package org.labs.qbit.debs.worker;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class BestPlayer {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> textIntWritableOutputCollector, Reporter reporter) throws IOException {

        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text text, Iterator<IntWritable> intWritableIterator, OutputCollector<Text, IntWritable> textIntWritableOutputCollector, Reporter reporter) throws IOException {

        }
    }

    public static void main(String[] args) {

    }
}
