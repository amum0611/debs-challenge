package org.labs.qbit.debs.core;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.labs.qbit.debs.core.Constants.*;

public class ParserUtil {

    public void perform(String filename) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(filename), 10 * 1024 * 1024);

        long count = 0;

        String line = br.readLine();

        long start = System.currentTimeMillis();

        long timerTime = 0;

        while (line != null) {

            String[] dataStr = line.split(",");

            line = br.readLine();
            //sid, ts (pico second 10^-12), x (mm), y(mm), z(mm), v (um/s 10^(-6)), a (us^-2), vx, vy, vz, ax, ay, az

            double v_kmh = Double.valueOf(dataStr[5]) * 60 * 60 / 1000000000;

            double a_ms = Double.valueOf(dataStr[6]) / 1000000;

            long time = Long.valueOf(dataStr[1]);

            Object[] data = new Object[]{dataStr[0], time, Double.valueOf(dataStr[2]),
                    Double.valueOf(dataStr[3]), Double.valueOf(dataStr[4]), v_kmh,
                    a_ms, Integer.valueOf(dataStr[7]), Integer.valueOf(dataStr[8]),
                    Integer.valueOf(dataStr[9]), Integer.valueOf(dataStr[10]), Integer.valueOf(dataStr[11]), Integer.valueOf(dataStr[12]),
                    System.nanoTime(), ((Double) (time * Math.pow(10, -9))).longValue()};
        }
    }

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