package org.checkpoint;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CheckpointingExample {
    public static void checkpointingExample(StreamExecutionEnvironment stEnv, ParameterTool params)
            throws Exception {
        stEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // start a checkpoint every 1000 ms
        stEnv.enableCheckpointing(1000);

        // to set minimum progress time to happen between checkpoints
        stEnv.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // checkpoints have to complete within 10000 ms, or are discarded
        stEnv.getCheckpointConfig().setCheckpointTimeout(10000);

        // set mode to exactly-once (this is the default)
        stEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);  // AT_LEAST_ONCE

        // allow only one checkpoint to be in progress at the same time
        stEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // enable externalized checkpoints which are retained after job cancellation
        stEnv.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);  // DELETE_ON_CANCELLATION

        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        stEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(	3, 100 ));
        // number of restart attempts , delay in each restart

        // go to the flink app overview and see checkpoints tab

        DataStream<String> data = stEnv.socketTextStream("localhost", 9090);

        DataStream<Long> sum = data.map(new MapFunction<String, Tuple2<Long, String>>()
                {
                    public Tuple2<Long, String> map(String s)
                    {
                        String[] words = s.split(",");
                        return new Tuple2<Long, String>(Long.parseLong(words[0]), words[1]);
                    }
                })
                .keyBy(0)
                .flatMap(new StatefulMap());
        sum.writeAsText("<output-path>/checkpoint_example_op");

        // execute program
        stEnv.execute("Value State Example");
    }

    public static class StatefulMap extends RichFlatMapFunction<Tuple2<Long, String>, Long>
    {
        private transient ValueState<Long> sum;            // 2
        private transient ValueState<Long> count;          //  4

        public void flatMap(Tuple2<Long, String> input, Collector<Long> out)throws Exception
        {
            Long currCount = count.value();        //   2
            Long currSum = sum.value();             //  4

            currCount += 1;
            currSum = currSum + Long.parseLong(input.f1);

            count.update(currCount);
            sum.update(currSum);

            if (currCount >= 5)
            {
                /* emit sum of last 5 elements */
                out.collect(sum.value());
                /* clear value */
                count.clear();
                sum.clear();
            }
        }
        public void open(Configuration conf)
        {
            ValueStateDescriptor<Long> descriptor =new ValueStateDescriptor<Long>("sum", TypeInformation.of(new TypeHint<Long>() {}), 0L);
            sum = getRuntimeContext().getState(descriptor);

            ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<Long>( "count",  TypeInformation.of(new TypeHint<Long>() {}), 0L);
            count = getRuntimeContext().getState(descriptor2);
        }
    }
}