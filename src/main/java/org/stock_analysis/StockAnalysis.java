// Problem Statement

// For every minute, calculate
//
//        a.) Maximum trade price
//        b.) Minimum trade price
//        c.) Maximum trade volume
//        d.) Minimum trade volume
//        e.) % change in Max trade price from previous 1 minute
//        f.) % change in Max trade volume from previous 1 minute
//
//        1st Report looks like
//        From Timestamp	End Timestamp	Currt Wndw Max price	Currt Wndw Min price	% change in Max price	Currt Wndw Max Vol	Currt Wndw Min Vol	% change in Max Vol
//        06/10/2010:08:00:00	06/10/2010:08:00:59	107.0	101.5	0.00	354881	330164	0.00
//        06/10/2010:08:01:00	06/10/2010:08:01:59	103.0	101.0	-3.74	354948	330514	0.02
//        06/10/2010:08:02:00	06/10/2010:08:02:59	106.0	102.5	2.91	354834	332433	-0.03
//        06/10/2010:08:03:00	06/10/2010:08:03:59	107.5	105.5	1.42	354922	331050	0.02
//        06/10/2010:08:04:00	06/10/2010:08:04:59	107.0	104.5	-0.47	354851	330202	-0.02
//        06/10/2010:08:05:00	06/10/2010:08:05:59	106.0	104.5	-0.93	353795	337712	-0.30
//        2nd Report (Alert report)
//
//        For every 5 minute window, if the Max trade price is changing (up or down) by more than 5%, then record that event
//
//        % Change	Prev Wndw Max price	Currt Wndw Max price	Event timestamp
//        Large Change Detected of -7.83%	(115.0)	(106.0)	06/10/2010:09:20:00
//        Large Change Detected of 10.26%	(107.0)	(118.0)	06/10/2010:09:34:48
//        Large Change Detected of 7.00%	(121.5)	(130.0)	06/10/2010:09:48:02

package org.stock_analysis;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

public class StockAnalysis {
    public static void runStockAnalysis(StreamExecutionEnvironment stEnv, ParameterTool params)
            throws Exception {
        stEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple5<String, String, String, Double, Integer>> data = stEnv.readTextFile("/resources/FUTURES_TRADES.txt")
                .map(new MapFunction<String, Tuple5<String, String, String, Double, Integer>>() {
                    public Tuple5<String, String, String, Double, Integer> map(String value) {
                        String[] words = value.split(",");
                        // date,    time,     Name,       trade,                      volume
                        return new Tuple5<String, String, String, Double, Integer>(words[0], words[1], "XYZ", Double.parseDouble(words[2]), Integer.parseInt(words[3]));
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple5<String, String, String, Double, Integer>>() {
                    private final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

                    public long extractAscendingTimestamp(Tuple5<String, String, String, Double, Integer> value) {
                        try {
                            Timestamp ts = new Timestamp(sdf.parse(value.f0 + " " + value.f1).getTime());

                            return ts.getTime();
                        } catch (Exception e) {
                            throw new RuntimeException("Parsing Error");
                        }
                    }
                });

        // Compute per window statistics
        DataStream<String> change = data.keyBy(new KeySelector<Tuple5<String, String, String, Double, Integer>, String>() {
                    public String getKey(Tuple5<String, String, String, Double, Integer> value) {
                        return value.f2;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new TrackChange());
        change.writeAsText("<output-path>/output/stock_analysis_first_report.txt");

        // Alert when price change from one window to another is more than threshold
        DataStream<String> largeDelta = data.keyBy(new KeySelector<Tuple5<String, String, String, Double, Integer>, String>() {
                    public String getKey(Tuple5<String, String, String, Double, Integer> value) {
                        return value.f2;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new TrackLargeDelta(5));
        largeDelta.writeAsText("<output-path>/output/stock_analysis_first_alert.txt");

        stEnv.execute("Stock Analysis");
    }

    public static class TrackChange extends ProcessWindowFunction<Tuple5<String, String, String, Double, Integer>, String, String, TimeWindow> {
        private transient ValueState<Double> prevWindowMaxTrade;
        private transient ValueState<Integer> prevWindowMaxVol;

        public void process(String key, Context context, Iterable<Tuple5<String, String, String, Double, Integer>> input, Collector<String> out) throws Exception {
            String windowStart = "";
            String windowEnd = "";
            Double windowMaxTrade = 0.0;              // 0
            Double windowMinTrade = 0.0;              // 106
            Integer windowMaxVol = 0;
            Integer windowMinVol = 0;                 // 348746

            for (Tuple5<String, String, String, Double, Integer> element : input)
            //  06/10/2010, 08:00:00, 106.0, 348746
            //  06/10/2010, 08:00:00, 105.0, 331580
            {
                if (windowStart.isEmpty()) {
                    windowStart = element.f0 + ":" + element.f1;   // 06/10/2010 : 08:00:00
                    windowMinTrade = element.f3;
                    windowMinVol = element.f4;
                }
                if (element.f3 > windowMaxTrade) {
                    windowMaxTrade = element.f3;
                }

                if (element.f3 < windowMinTrade) {
                    windowMinTrade = element.f3;
                }

                if (element.f4 > windowMaxVol) {
                    windowMaxVol = element.f4;
                }
                if (element.f4 < windowMinVol) {
                    windowMinVol = element.f4;
                }

                windowEnd = element.f0 + ":" + element.f1;
            }

            Double maxTradeChange = 0.0;
            Double maxVolChange = 0.0;

            if (prevWindowMaxTrade.value() != 0) {
                maxTradeChange = ((windowMaxTrade - prevWindowMaxTrade.value()) / prevWindowMaxTrade.value()) * 100;
            }
            if (prevWindowMaxVol.value() != 0) {
                maxVolChange = ((windowMaxVol - prevWindowMaxVol.value()) * 1.0 / prevWindowMaxVol.value()) * 100;
            }

            out.collect(windowStart + " - " + windowEnd + ", " + windowMaxTrade + ", " + windowMinTrade + ", " + String.format("%.2f", maxTradeChange)
                    + ", " + windowMaxVol + ", " + windowMinVol + ", " + String.format("%.2f", maxVolChange));

            prevWindowMaxTrade.update(windowMaxTrade);
            prevWindowMaxVol.update(windowMaxVol);
        }

        public void open(Configuration config) {
            prevWindowMaxTrade = getRuntimeContext().getState(new ValueStateDescriptor<Double>("prev_max_trade", BasicTypeInfo.DOUBLE_TYPE_INFO, 0.0));

            prevWindowMaxVol = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("prev_max_vol", BasicTypeInfo.INT_TYPE_INFO, 0));
        }
    }

    public static class TrackLargeDelta extends ProcessWindowFunction<Tuple5<String, String, String, Double, Integer>, String, String, TimeWindow> {
        private final double threshold;
        private transient ValueState<Double> prevWindowMaxTrade;

        public TrackLargeDelta(double threshold) {
            this.threshold = threshold;
        }

        public void process(String key, Context context, Iterable<Tuple5<String, String, String, Double, Integer>> input, Collector<String> out) throws Exception {
            Double prevMax = prevWindowMaxTrade.value();
            Double currMax = 0.0;
            String currMaxTimeStamp = "";

            for (Tuple5<String, String, String, Double, Integer> element : input) {
                if (element.f3 > currMax) {
                    currMax = element.f3;
                    currMaxTimeStamp = element.f0 + ":" + element.f1;
                }
            }

            // check if change is more than specified threshold
            Double maxTradePriceChange = ((currMax - prevMax) / prevMax) * 100;

            if (prevMax != 0 &&  // don't calculate delta the first time
                    Math.abs((currMax - prevMax) / prevMax) * 100 > threshold) // Math.abs is used to calculate negative percent change
            {
                out.collect("Large Change Detected of " + String.format("%.2f", maxTradePriceChange) + "%" + " (" + prevMax + " - " + currMax + ") at  " + currMaxTimeStamp);
            }
            prevWindowMaxTrade.update(currMax);
        }

        public void open(Configuration config) {
            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<Double>("prev_max", BasicTypeInfo.DOUBLE_TYPE_INFO, 0.0);
            prevWindowMaxTrade = getRuntimeContext().getState(descriptor);
        }
    }
}
