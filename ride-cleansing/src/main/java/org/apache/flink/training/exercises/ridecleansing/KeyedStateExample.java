package org.apache.flink.training.exercises.ridecleansing;

import datatypes.Event;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;

public class KeyedStateExample {

    static final Event[] events;
    static
    {
        LocalDateTime now = LocalDateTime.now();
         events = new Event[]
                {
                        new Event("key1", getLongFromDate(now, 0)),
                        new Event("key2", getLongFromDate(now, 10)),
                        new Event("key3", getLongFromDate(now, 20)),
                        new Event("key4", getLongFromDate(now, 30)),
                        new Event("key5", getLongFromDate(now, 40)),
                        new Event("key6", getLongFromDate(now, 50)),
                        new Event("key1", getLongFromDate(now, 60)),
                        new Event("key2", getLongFromDate(now, 70)),
                        new Event("key3", getLongFromDate(now, 80)),
                        new Event("key4", getLongFromDate(now, 90)),
                        new Event("key5", getLongFromDate(now, 100))
                };
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExerciseBase.parallelism);



        env.fromElements(events)
           .keyBy(Event::getKey)
           .flatMap(new Deduplicator())
           .print();

        env.execute("Event ");

    }

    private static Long getLongFromDate(LocalDateTime dateTime, int minutesToAdd)
    {
        LocalDateTime newDateTime = minutesToAdd == 0 ? dateTime : dateTime.plus(Duration.ofMinutes(minutesToAdd));
        return Timestamp.valueOf(dateTime).getTime();
    }

    static class Deduplicator extends RichFlatMapFunction<Event, Event> {
        private static final long serialVersionUID = 1616982862220165524L;
        ValueState<Boolean> keyHasBeenSeen;

        @Override
        public void open(Configuration conf) {
            ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("keyHasBeenSeen", Types.BOOLEAN);
            keyHasBeenSeen = getRuntimeContext().getState(desc);
        }

        @Override
        public void flatMap(Event event, Collector<Event> out) throws Exception {
            if (keyHasBeenSeen.value() == null) {
                out.collect(event);
                keyHasBeenSeen.update(true);
            }
        }
    }
}
