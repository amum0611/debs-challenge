package org.labs.qbit.debs.worker;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.labs.qbit.debs.core.Constants;
import org.labs.qbit.debs.core.ParserUtil;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class BallMovement {

    public static String[] ballSensors = new String[]{"4", "8", "10", "12"};

    public static class MapFunction extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

        private Text sensorIdText = new Text();
        private Text coordinatesText = new Text();

        @Override
        public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            Map<String,Object> map = ParserUtil.parse(line);
            String sensorId = String.valueOf(map.get(Constants.SENSOR_ID));
            Double x = (Double) map.get(Constants.X);
            Double y = (Double) map.get(Constants.Y);
            //is the sensorId belongs to one of the balls?
            if (contains(sensorId, ballSensors)) {
                sensorIdText.set(sensorId);
                coordinatesText.set(String.format("%s,%s", x, y));
                output.collect(sensorIdText, coordinatesText);
            }
        }
    }

    public static class ReduceFunction extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        }
    }
    private static boolean contains(String value, String[] valueList) {
        for (String item : valueList) {
            if (item.equalsIgnoreCase(value)) {
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) {

    }
}
