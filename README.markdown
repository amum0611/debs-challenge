# Introduction

DEBS Challenge

* Case 1: Find the location of ball over time and plot the movement of the ball over the stadium. For each point where ball is, print a
point.

* Case2: Find out the best players by calculating how long each player spent near the 2m within the ball.

# License

* [Apache Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

    Developer: Azeem Mumtaz - 138218R

# Prerequisites

* Java 1.6

# Compile

    Compile the project into a jar, which contains classes of org.labs.qbit.*

# Simulation

* CASE 1

    In the map function, a sensor data related to the ball is parsed and its X and Y coordinates are emitted
    to the reducer. From the reducer a value X and a list of [Y]s are received and emitted X as the ket and each Y as
    values separately.

    OUTPUT: A file which contains X,Y coordinates of balls. This can be used to plot the graph.

    Related Job Class: org.labs.qbit.debs.worker.BallMovement

    Execute:

        bin/hadoop jar debs-challenge.jar org.labs.qbit.debs.worker.BallMovement  $INPUT_DIR $OUTPUT_DIR

* CASE 2

    This is a 2 step process.

    (i) Step 1 -

        From the given data, each data related to a given timestamp is emitted. The idea is that, when it comes to the reducer,
        it will have a timestamp and a list of sensor data. This list of data means that the snapshot of the playground
        on the given time. Now, at reducer, we can find the data related to the ball, and calculate the distance of all other
        sensor data, and determine whether the distance is within 2 meters. If so, emit the sensorId and 1.

        The output of this is fed to {@link BestPlayer} job.

        Related Job Class: org.labs.qbit.debs.worker.TimeSnapshot

        Execute:

            bin/hadoop jar debs-challenge.jar org.labs.qbit.debs.worker.TimeSnapshot  $INPUT_DIR $OUTPUT_DIR


    (i) Step 2 -

        Given dataset, which contains set of "sensorId and 1" that is the output of {@link TimeSnapshot} step 1 job,
        reduce list of 1s to the sum. The highest sum indicates that the sensor, who has spent the maximum number of
        times near 2 meters perimeter of the ball.

        The output of this is fed to {@link BestPlayer} job.

        Related Job Class: org.labs.qbit.debs.worker.BestPlayer

        Execute:

            bin/hadoop jar debs-challenge.jar org.labs.qbit.debs.worker.BestPlayer  $INPUT_DIR $OUTPUT_DIR

## Pre conditions

    Hadoop 1.2.1