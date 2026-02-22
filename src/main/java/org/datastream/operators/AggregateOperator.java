package org.datastream.operators;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;

public class AggregateOperator {
    public static void aggregateOperator(StreamExecutionEnvironment stEnv, ParameterTool params) throws Exception {
        DataStream<String> data = stEnv.readTextFile("<input-path>/avg");
        // month, category,product, profit,
        DataStream<Tuple4<String, String, String, Integer>> mapped = data.map(new Splitter()); // tuple  [June,Category5,Bat,12]
        //       [June,Category4,Perfume,10,1]
        mapped.keyBy(new KeySelector<Tuple4<String, String, String, Integer>, String>() {
            @Override
            public String getKey(Tuple4<String, String, String, Integer> value) {
                return value.f0;
            }
        }).sum(3).writeAsText("<output-path>/output/agg_sum");

        mapped.keyBy(new KeySelector<Tuple4<String, String, String, Integer>, String>() {
            @Override
            public String getKey(Tuple4<String, String, String, Integer> value) {
                return value.f0;
            }
        }).min(3).writeAsText("<output-path>/output/agg_min");

        mapped.keyBy(new KeySelector<Tuple4<String, String, String, Integer>, String>() {
            @Override
            public String getKey(Tuple4<String, String, String, Integer> value) {
                return value.f0;
            }
        }).minBy(3).writeAsText("<output-path>/output/agg_minBy");

        mapped.keyBy(new KeySelector<Tuple4<String, String, String, Integer>, String>() {
            @Override
            public String getKey(Tuple4<String, String, String, Integer> value) {
                return value.f0;
            }
        }).max(3).writeAsText("<output-path>/output/agg_max");

        mapped.keyBy(new KeySelector<Tuple4<String, String, String, Integer>, String>() {
            @Override
            public String getKey(Tuple4<String, String, String, Integer> value) {
                return value.f0;
            }
        }).maxBy(3).writeAsText("<output-path>/output/agg_maxBy");

        // execute program
        stEnv.execute("Aggregate Operator");
    }

    public static class Splitter implements MapFunction<String, Tuple4<String, String, String, Integer>> {
        public Tuple4<String, String, String, Integer> map(String value) // 01-06-2018,June,Category5,Bat,12
        {
            String[] words = value.split(","); // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
            // ignore timestamp, we don't need it for any calculations
            return new Tuple4<String, String, String, Integer>(words[1], words[2], words[3], Integer.parseInt(words[4]));
        } //    June    Category5      Bat               12
    }
}
