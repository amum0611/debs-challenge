package org.labs.qbit.debs.worker;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.labs.qbit.debs.core.Constants.SENSOR_ID;
import static org.labs.qbit.debs.core.Constants.X;
import static org.labs.qbit.debs.core.Constants.Y;
import static org.labs.qbit.debs.worker.BallMovement.ballSensors;
import static org.labs.qbit.debs.worker.BallMovement.contains;

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
 * From the given data, each data related to a given timestamp is emitted. The idea is that, when it comes to the reducer,
 * it will have a timestamp and a list of sensor data. This list of data means that the snapshot of the playground
 * on the given time. Now, at reducer, we can find the data related to the ball, and calculate the distance of all other
 * sensor data, and determine whether the distance is within 2 meters. If so, emit the sensorId and 1.
 *
 * The output of this is fed to {@link BestPlayer} job.
 */
public class TimeSnapshot {

    private static final String JOB_NAME = "SNAPSHOT_OF_SENSORS_BY_TIMESTAMP";

    private static final Double BEST_DISTANCE = 2000d;

    public static class MapFunction extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

        private LongWritable timeWritable = new LongWritable();

        @Override
        public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            Map<String, Object> map = ParserUtil.parse(line);
            Long time = (Long) map.get(Constants.TIME);
            timeWritable.set(time);
            output.collect(timeWritable, new Text(value));
        }
    }

    public static class ReduceFunction extends MapReduceBase implements Reducer<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Map<String, Object> ballDataMap = new HashMap<String, Object>();

        @Override
        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();
            while (values.hasNext()) {
                Text next = values.next();
                Map<String, Object> map = ParserUtil.parse(next.toString());
                String sensorId = String.valueOf(map.get(SENSOR_ID));
                //is the sensorId belongs to one of the balls?
                if (contains(sensorId, ballSensors)) {
                    //Attn: it is assumed that at any given moment of time, there is only one data related to ball sensor
                    ballDataMap = new HashMap<String, Object>(map);
                } else {
                    mapList.add(map);
                }
            }

            if (ballDataMap.containsKey(SENSOR_ID)) {
                for (Map<String, Object> map : mapList) {
                    Text sensorId = new Text(String.valueOf(map.get(SENSOR_ID)));
                    Double distance = distanceXY((Double) ballDataMap.get(X), (Double) ballDataMap.get(Y), (Double) map.get(X), (Double) map.get(Y));
                    if (distance <= BEST_DISTANCE) {
                        output.collect(sensorId, one);
                    }
                }
            }
        }
    }

    public static Double distanceXY(Double ballX, Double ballY, Double playerX, Double playerY) {
        return Math.sqrt(Math.pow(ballX - playerX, 2) + Math.pow(ballY - playerY, 2));
    }

    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName(JOB_NAME);

        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(MapFunction.class);
//        conf.setCombinerClass(ReduceFunction.class);
        conf.setReducerClass(ReduceFunction.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
