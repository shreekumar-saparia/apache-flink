package org.datastream.operators;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;

public class IterateOperator {
    public static void iterateOperator(StreamExecutionEnvironment stEnv, ParameterTool params) throws Exception {
        DataStream<Tuple2<Long, Integer>> data =
                stEnv.generateSequence(0, 5).map(new MapFunction<Long, Tuple2<Long, Integer>>() {
                    public Tuple2<Long, Integer> map(Long value) {
                        return new Tuple2<Long, Integer>(value, 0);
                    }
                });

        // prepare stream for iteration
        IterativeStream<Tuple2<Long, Integer>> iteration = data.iterate(5000); // ( 0,0   1,0  2,0  3,0   4,0  5,0 )

        // define iteration
        DataStream<Tuple2<Long, Integer>> plusOne =
                iteration.map(new MapFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>>() {
                    public Tuple2<Long, Integer> map(Tuple2<Long, Integer> value) {
                        if (value.f0 == 10) {
                            return value;
                        } else {
                            return new Tuple2<Long, Integer>(value.f0 + 1, value.f1 + 1);
                        }
                    }
                }); //   plusone    1,1   2,1  3,1   4,1   5,1   6,1

        // part of stream to be used in next iteration (
        DataStream<Tuple2<Long, Integer>> notEqualtoten =
                plusOne.filter(new FilterFunction<Tuple2<Long, Integer>>() {
                    public boolean filter(Tuple2<Long, Integer> value) {
                        if (value.f0 == 10) {
                            return false;
                        } else {
                            return true;
                        }
                    }
                });
        // feed data back to next iteration
        iteration.closeWith(notEqualtoten);

        // data not feedback to iteration
        DataStream<Tuple2<Long, Integer>> equaltoten =
                plusOne.filter(new FilterFunction<Tuple2<Long, Integer>>() {
                    public boolean filter(Tuple2<Long, Integer> value) {
                        if (value.f0 == 10) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                });

        equaltoten.writeAsText("<output-path>/output/iterate_operator_op");

        stEnv.execute("Iteration Demo");
    }
}
