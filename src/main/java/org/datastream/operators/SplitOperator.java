package org.datastream.operators;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.functions.ProcessFunction;

public class SplitOperator {
    public static void splitOperator(StreamExecutionEnvironment stEnv, ParameterTool params) throws Exception {
        DataStream<String> text = stEnv.readTextFile("<input-path>/oddeven");

        // String type side output for Even values
        final OutputTag<String> evenOutTag = new OutputTag<String>("even-string-output") {
        };
        // Integer type side output for Odd values
        final OutputTag<Integer> oddOutTag = new OutputTag<Integer>("odd-int-output") {
        };

        SingleOutputStreamOperator<Integer> mainStream = text
                .process(new ProcessFunction<String, Integer>() {
                    @Override
                    public void processElement(
                            String value,
                            Context ctx,
                            Collector<Integer> out) throws Exception {

                        int intVal = Integer.parseInt(value);

                        // get all data in regular output as well
                        out.collect(intVal);

                        if (intVal % 2 == 0) {
                            // emit data to side output for even output
                            ctx.output(evenOutTag, String.valueOf(intVal));
                        } else {
                            // emit data to side output for even output
                            ctx.output(oddOutTag, intVal);
                        }
                    }
                });

        DataStream<String> evenSideOutputStream = mainStream.getSideOutput(evenOutTag);
        DataStream<Integer> oddSideOutputStream = mainStream.getSideOutput(oddOutTag);

        evenSideOutputStream.writeAsText("<output-path>/output/split_operator_even");
        oddSideOutputStream.writeAsText("<output-path>/output/split_operator_odd");

        // execute program
        stEnv.execute("Split Operator ODD EVEN");

    }
}
