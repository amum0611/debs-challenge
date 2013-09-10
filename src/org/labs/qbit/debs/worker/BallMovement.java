package org.labs.qbit.debs.worker;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.labs.qbit.debs.core.Constants;
import org.labs.qbit.debs.core.ParserUtil;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Copyright (c) 2013, QBit-Labs Inc. (http://qbit-labs.org) All Rights Reserved.
 *
 * QBit-Labs Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *
 * In the map function, a sensor data related to the ball is parsed and its X and Y coordinates are emitted
 * to the reducer. From the reducer a value X and a list of [Y]s are received and emitted X as the ket and each Y as
 * values separately.
 *
 * OUTPUT: A file which contains X,Y coordinates of balls. This can be used to plot the graph.
 */
public class BallMovement {

    public static final String[] ballSensors = new String[]{"4", "8", "10", "12"};
    private static final String JOB_NAME = "BALL_MOVEMENT";

    public static class MapFunction extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, DoubleWritable> {

        private DoubleWritable xText = new DoubleWritable();
        private DoubleWritable yText = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, DoubleWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            Map<String, Object> map = ParserUtil.parse(line);
            String sensorId = String.valueOf(map.get(Constants.SENSOR_ID));
            double x = (Double) map.get(Constants.X);
            double y = (Double) map.get(Constants.Y);
            //is the sensorId belongs to one of the balls?
            if (contains(sensorId, ballSensors)) {
                xText.set(x);
                yText.set(y);
                output.collect(xText, yText);
            }
        }
    }

    public static class ReduceFunction extends MapReduceBase implements Reducer<DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable> {

        @Override
        public void reduce(DoubleWritable key, Iterator<DoubleWritable> values, OutputCollector<DoubleWritable, DoubleWritable> output, Reporter reporter) throws IOException {
            while (values.hasNext()) {
                output.collect(key, values.next());
            }
        }
    }

    public static boolean contains(String value, String[] valueList) {
        for (String item : valueList) {
            if (item.equalsIgnoreCase(value)) {
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName(JOB_NAME);

        conf.setOutputKeyClass(DoubleWritable.class);
        conf.setOutputValueClass(DoubleWritable.class);

        conf.setMapperClass(MapFunction.class);
        conf.setCombinerClass(ReduceFunction.class);
        conf.setReducerClass(ReduceFunction.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
