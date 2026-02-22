package org.realtime;

import org.apache.flink.api.java.utils.ParameterTool;
import java.util.Properties;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ConsumeTwitterData {
    public static void consumeTwitterData(StreamExecutionEnvironment stEnv, ParameterTool params)
            throws Exception {
        stEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties twitterCredentials = new Properties();
        twitterCredentials.setProperty(TwitterSource.CONSUMER_KEY, "");
        twitterCredentials.setProperty(TwitterSource.CONSUMER_SECRET, "");
        twitterCredentials.setProperty(TwitterSource.TOKEN, "");
        twitterCredentials.setProperty(TwitterSource.TOKEN_SECRET, "");

        DataStream<String> twitterData = stEnv.addSource(new TwitterSource(twitterCredentials));

        twitterData.flatMap(new TweetParser()).writeAsText("<output-path>/twitter_data_op");

        stEnv.execute("Consume Twitter Data From API");
    }

    public static class TweetParser implements FlatMapFunction<String, Tuple2<String, Integer>> {

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            ObjectMapper jsonParser = new ObjectMapper();
            JsonNode node = jsonParser.readValue(value, JsonNode.class);

            boolean isEnglish =
                    node.has("user") &&
                            node.get("user").has("lang") &&
                            node.get("user").get("lang").asText().equals("en");

            boolean hasText = node.has("text");

            if (isEnglish && hasText) {
                String tweet = node.get("text").asText();

                out.collect(new Tuple2<String, Integer>(tweet, 1));
            }
        }
    }
}
