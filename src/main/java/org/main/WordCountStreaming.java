package org.main;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class WordCountStreaming {
    public static void wordCountStreaming(StreamExecutionEnvironment stEnv, ParameterTool params) throws Exception {
        DataStream<String> text = stEnv.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Integer>> counts = text.filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) {
                        return value.startsWith("N");
                    }
                }).map(new Tokenizer()) // split up the lines in pairs (2-tuples) containing: tuple2 {(name,1)...}
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) {
                        return value.f0;
                    }
                })
                .sum(1); // group by the tuple field "0" and sum up tuple field "1"

        counts.print();
        stEnv.execute("Word Count Streaming");
    }

    public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {
        public Tuple2<String, Integer> map(String value) {
            return new Tuple2<String, Integer>(value, Integer.valueOf(1));
        }
    }
}
