package org.apache.flink.training.exercises.ridecleansing;

import datatypes.EnrichedRide;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class RideCleansingExtended extends ExerciseBase{
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExerciseBase.parallelism);

        // start the data generator
        TaxiRideGenerator source = new TaxiRideGenerator();

        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(source));

        /* this is variant using filter and map function
        DataStream<EnrichedRide> filteredRides = rides
                // filter out rides that do not start or stop in NYC
                .filter(new RideCleansingExercise.NYCFilter())
                .map(new Enrichment());*/

        //this is variant is using flatmap
        DataStream<EnrichedRide> filteredRides = rides
                .flatMap(new NYCEnrichment())
                //this grouping results by startCell
                //for example
                // EnrichedRide with startCell 23200
                // EnrichedRide with startCell 23201
                // EnrichedRide with startCell 23200
                // EnrichedRide with startCell 23001
                // will be grouped to
                // EnrichedRide with startCell 23200
                // EnrichedRide with startCell 23200
                // EnrichedRide with startCell 23201
                // EnrichedRide with startCell 23001
                //all records with the same key are assigned to the same subtask
                .keyBy(enrichedRide -> enrichedRide.rideId);


        // print the filtered stream
        //filteredRides.writeAsText("D:\\result");
        printOrTest(filteredRides);

        //Output stream now contains a record for each key every time the duration reaches a new maximum
        DataStreamSink<Tuple2<Integer, Long>> minutesByStartCell = filteredRides
                .flatMap(new getMinutesByStartCell())
                .keyBy(value -> value.f0) // .keyBy(value -> value.startCell)
                .maxBy(1) //duration
                .writeAsText("D:\\\\result1");


        // run the cleansing pipeline
        env.execute("Taxi Ride Cleansing with Enrichment");

    }

    static class Enrichment implements MapFunction<TaxiRide, EnrichedRide>
    {
        @Override
        public EnrichedRide map(TaxiRide value) {
            return new EnrichedRide(value);
        }
    }

    static class NYCEnrichment implements FlatMapFunction<TaxiRide, EnrichedRide>
    {
        @Override
        public void flatMap(TaxiRide value, Collector<EnrichedRide> out) throws Exception {
            FilterFunction<TaxiRide> NYCFilter = new RideCleansingExercise.NYCFilter();
            if (NYCFilter.filter(value))
            {
                out.collect(new EnrichedRide(value));
            }
        }
    }

    static class getMinutesByStartCell implements FlatMapFunction<EnrichedRide, Tuple2<Integer, Long>>
    {
        @Override
        public void flatMap(EnrichedRide value, Collector<Tuple2<Integer, Long>> out) throws Exception {
            if (!value.isStart)
            {
                Duration duration = Duration.between(value.startTime, value.endTime);
                Long durationInMinutes = Math.abs(duration.toMinutes());
                out.collect(new Tuple2<>(value.startCell, durationInMinutes));
            }
        }
    }
}
