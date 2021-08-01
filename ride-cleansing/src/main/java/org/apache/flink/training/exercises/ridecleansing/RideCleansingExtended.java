package org.apache.flink.training.exercises.ridecleansing;

import datatypes.EnrichedRide;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.util.Collector;

public class RideCleansingExtended extends ExerciseBase{
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExerciseBase.parallelism);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()));

        /* this is variant using filter and map function
        DataStream<EnrichedRide> filteredRides = rides
                // filter out rides that do not start or stop in NYC
                .filter(new RideCleansingExercise.NYCFilter())
                .map(new Enrichment());*/

        //this is variant is using flatmap
        DataStream<EnrichedRide> filteredRides = rides
                .flatMap(new NYCEnrichment());


        // print the filtered stream
        printOrTest(filteredRides);

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
}
