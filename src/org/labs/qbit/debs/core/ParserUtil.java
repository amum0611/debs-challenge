package org.labs.qbit.debs.core;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.labs.qbit.debs.core.Constants.*;

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
 */
public class ParserUtil {

    public static Map<String, Object> parse(String line) {
        String[] dataStr = line.split(",");
        double v_kmh = Double.valueOf(dataStr[5]) * 60 * 60 / 1000000000;

        double a_ms = Double.valueOf(dataStr[6]) / 1000000;

        long time = Long.valueOf(dataStr[1]);

        Map<String, Object> map = new HashMap<String, Object>();
        map.put(SENSOR_ID, dataStr[0]);
        map.put(TIME, time);
        map.put(X, Double.valueOf(dataStr[2]));
        map.put(Y, Double.valueOf(dataStr[3]));
        map.put(Z, Double.valueOf(dataStr[4]));
        map.put(ABSOLUTE_VELOCITY, v_kmh);
        map.put(ABSOLUTE_ACCELERATION, a_ms);
        map.put(VX, Integer.valueOf(dataStr[7]));
        map.put(VY, Integer.valueOf(dataStr[8]));
        map.put(VZ, Integer.valueOf(dataStr[9]));
        map.put(AX, Integer.valueOf(dataStr[10]));
        map.put(AY, Integer.valueOf(dataStr[11]));
        map.put(AZ, Integer.valueOf(dataStr[12]));
        map.put(NANO_TIME, System.nanoTime());
        map.put(ELAPSE_TIME, ((Double) (time * Math.pow(10, -9))).longValue());

        return map;
    }
}