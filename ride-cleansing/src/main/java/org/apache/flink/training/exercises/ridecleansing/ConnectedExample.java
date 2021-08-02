package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class ConnectedExample {
    // a control stream is used to specify words which must be filtered out of the streamOfWords
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> control = env
                .fromElements("DROP", "IGNORE")
                .keyBy(x -> x);

        DataStream<String> streamOfWords = env
                .fromElements("Apache", "DROP", "Flink", "IGNORE")
                .keyBy(x -> x);

        // flatMap1 and flatMap2 are called by the Flink runtime with elements from each of the two connected streams
        // – in our case, elements from the control stream are passed into flatMap1, and elements from streamOfWords are
        // passed into flatMap2.
        // This was determined by the order in which the two streams are connected with control.connect(streamOfWords).
        control
                .connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print();

        env.execute();
    }

    static class ControlFunction extends RichCoFlatMapFunction<String, String, String>
    {
        private static final long serialVersionUID = 3177193374650975416L;
        private ValueState<Boolean> blocked;

        @Override
        public void open(Configuration parameters) {
            //The blocked Boolean is being used to remember the keys (words, in this case) that have been mentioned
            //on the control stream, and those words are being filtered out of the streamOfWords stream.
            blocked = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("blocked", Boolean.class));
        }

        // no order
        // if you are truly desperate, it is possible to exert some limited control over the order in which a two-input
        // operator consumes its inputs by using a custom Operator that implements the InputSelectable interface.
        @Override
        public void flatMap1(String controlValue, Collector<String> out) throws Exception {
            blocked.update(Boolean.TRUE);
        }

        @Override
        public void flatMap2(String dataValue, Collector<String> out) throws Exception {
            if (blocked.value() == null)
            {
                out.collect(dataValue);
            }
        }
    }
}
