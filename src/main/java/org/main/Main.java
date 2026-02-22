package org.main;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.checkpoint.CheckpointingExample;
import org.dataset.transformation.FullOuterJoin;
import org.dataset.transformation.InnerJoin;
import org.dataset.transformation.LeftOuterJoin;
import org.dataset.transformation.RightOuterJoin;
import org.datastream.operators.AggregateOperator;
import org.datastream.operators.IterateOperator;
import org.datastream.operators.ReduceOperator;
import org.datastream.operators.SplitOperator;
import org.producer.DataProducer;
import org.producer.TimestampDataProducer;
import org.realtime.ConsumeTwitterData;
import org.realtime.KafkaSource;
import org.sql_api.SQLApiExample;
import org.states.ValueStateExample;
import org.stock_analysis.StockAnalysis;
import org.table_api.TableApiExample;
import org.windows.*;

public class Main {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        ExecutionEnvironment env = null;
        StreamExecutionEnvironment stEnv = null;
        String processType = params.get("processType");
        String operationType = params.get("operationType");


        if (processType.equalsIgnoreCase("batch")) {
            // set up the execution environment
            env = ExecutionEnvironment.getExecutionEnvironment();
            // make parameters available in the web interface
            env.getConfig().setGlobalJobParameters(params);
        } else {
            // set up the execution environment for streaming
            stEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            stEnv.getConfig().setGlobalJobParameters(params);
        }

        if (env != null && processType.equalsIgnoreCase("batch") && operationType.equalsIgnoreCase("wordcount")) {
            WordCount.wordCount(env, params);
        } else if (env != null && processType.equalsIgnoreCase("batch") && operationType.equalsIgnoreCase("inner_join")) {
            InnerJoin.innerJoin(env, params);
        } else if (env != null && processType.equalsIgnoreCase("batch") && operationType.equalsIgnoreCase("left_join")) {
            LeftOuterJoin.leftJoin(env, params);
        } else if (env != null && processType.equalsIgnoreCase("batch") && operationType.equalsIgnoreCase("right_join")) {
            RightOuterJoin.rightJoin(env, params);
        } else if (env != null && processType.equalsIgnoreCase("batch") && operationType.equalsIgnoreCase("full_join")) {
            FullOuterJoin.fullJoin(env, params);
        } else if (stEnv != null && processType.equalsIgnoreCase("streaming") && operationType.equalsIgnoreCase("word_count_streaming")) {
            WordCountStreaming.wordCountStreaming(stEnv, params);
        } else if (stEnv != null && processType.equalsIgnoreCase("streaming") && operationType.equalsIgnoreCase("reduce_operator_streaming")) {
            ReduceOperator.reduceOperator(stEnv, params);
        } else if (stEnv != null && processType.equalsIgnoreCase("streaming") && operationType.equalsIgnoreCase("aggregate_operator_streaming")) {
            AggregateOperator.aggregateOperator(stEnv, params);
        } else if (stEnv != null && processType.equalsIgnoreCase("streaming") && operationType.equalsIgnoreCase("split_operator_streaming")) {
            SplitOperator.splitOperator(stEnv, params);
        } else if (stEnv != null && processType.equalsIgnoreCase("streaming") && operationType.equalsIgnoreCase("iterate_operator_streaming")) {
            IterateOperator.iterateOperator(stEnv, params);
        } else if (stEnv != null && processType.equalsIgnoreCase("streaming") && operationType.equalsIgnoreCase("tumbling_window_processing_time_notion")) {
            TumblingProcessingTimeNotionWindows.tumblingWindowsWithProcessingTimeNotion(stEnv, params);
        } else if (stEnv != null && processType.equalsIgnoreCase("streaming") && operationType.equalsIgnoreCase("tumbling_window_event_time_notion")) {
            TumblingEventTimeNotionWindows.tumblingWindowsWithEventTimeNotion(stEnv, params);
        } else if (stEnv != null && processType.equalsIgnoreCase("streaming") && operationType.equalsIgnoreCase("sliding_window_processing_time_notion")) {
            SlidingProcessingTimeNotionWindows.slidingWindowsWithProcessingTimeNotion(stEnv, params);
        } else if (stEnv != null && processType.equalsIgnoreCase("streaming") && operationType.equalsIgnoreCase("session_window_processing_time_notion")) {
            SlidingProcessingTimeNotionWindows.slidingWindowsWithProcessingTimeNotion(stEnv, params);
        } else if (stEnv != null && processType.equalsIgnoreCase("streaming") && operationType.equalsIgnoreCase("global_window_processing_time_notion")) {
            GlobalProcessingTimeNotionWindows.globalWindowsWithProcessingTimeNotion(stEnv, params);
        } else if (stEnv != null && processType.equalsIgnoreCase("streaming") && operationType.equalsIgnoreCase("value_state_example")) {
            ValueStateExample.valueState(stEnv, params);
        } else if (stEnv != null && processType.equalsIgnoreCase("streaming") && operationType.equalsIgnoreCase("checkpointing_example")) {
            CheckpointingExample.checkpointingExample(stEnv, params);
        } else if (stEnv != null && processType.equalsIgnoreCase("streaming") && operationType.equalsIgnoreCase("consume_twitter_data")) {
            ConsumeTwitterData.consumeTwitterData(stEnv, params);
        } else if (stEnv != null && processType.equalsIgnoreCase("streaming") && operationType.equalsIgnoreCase("consume_from_kafka")) {
            KafkaSource.consumeKafkaData(stEnv, params);
        } else if (stEnv != null && processType.equalsIgnoreCase("streaming") && operationType.equalsIgnoreCase("run_stock_analysis")) {
            StockAnalysis.runStockAnalysis(stEnv, params);
        } else if (env != null && processType.equalsIgnoreCase("batch") && operationType.equalsIgnoreCase("table_api_example")) {
            TableApiExample.tableApiExample(env);
        } else if (env != null && processType.equalsIgnoreCase("batch") && operationType.equalsIgnoreCase("sql_api_example")) {
            SQLApiExample.sqlApiExample(env);
        } else if (processType.equalsIgnoreCase("publish")) {
            if (operationType.equalsIgnoreCase("produce_data")) {
                DataProducer.produceData();
            } else if (operationType.equalsIgnoreCase("produce_timestamp_data")) {
                TimestampDataProducer.produceTimestampData();
            }
        }

    }
}
