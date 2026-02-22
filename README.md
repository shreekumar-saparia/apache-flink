<h1>Apache Flink Getting Started</h1>

1) download hadoop bundled version of flink from https://archive.apache.org/dist/flink/flink-1.5.0/ . I am using flink-1.5.0-bin-scala_2.11.tgz

2) Unzip file with below command. Make sure you are using java-8 and macOS.
   tar zxf flink-1.5.0-bin-scala_2.11.tgz

3) go to conf dir and edit flink-conf.yaml for below configurations
   - jobmanager.rpc.address: localhost
   - jobmanager.rpc.port: 7001
   - rest.port: 8082
   - taskmanager.rpc.port: 0
   - taskmanager.data.port: 0
   - jobmanager.heap.mb: 1024
   - taskmanager.heap.mb: 1024
   - taskmanager.numberOfTaskSlots: 2

4) start flink cluster. You should have 1 task manager, 2 task slots.
   cd flink-1.5.0
   .bin/start-cluster.sh

5) verify this by using http://localhost:8082/

<h3>Use below commands to run class programs</h3>
<h5>word count</h5>
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType batch --operationType wordcount --input <input-path>/names.txt --output <output-path>/wc_result

<h5>inner join</h5>
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType batch --operationType inner_join --input1 <input-path>/person --input2 <input-path>/location --output <output-path>/inner_join_op

<h5>left join</h5>
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType batch --operationType left_join --input1 <input-path>/person --input2 <input-path>/location --output <output-path>/left_join_op

<h5>right join</h5>
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType batch --operationType right_join --input1 <input-path>/person --input2 <input-path>/location --output <output-path>/right_join_op

<h5>full join</h5>
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType batch --operationType full_join --input1 <input-path>/person --input2 <input-path>/location --output <output-path>/full_join_op


<h5>word count streaming</h5>
nc -l 9999
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType streaming --operationType word_count_streaming 

<h5>reduce operator</h5>
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType streaming --operationType reduce_operator_streaming 

<h5>aggregate operator</h5>
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType streaming --operationType aggregate_operator_streaming

<h5>split operator</h5>
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType streaming --operationType split_operator_streaming

<h5>iterate operator</h5>
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType streaming --operationType iterate_operator_streaming

<h5>Produce data from avg file into the socket</h5>
we need flink libs at classpath because we are embedding flink code as well
java -cp "<jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar:<flink-library-path>/flink-1.5.0/lib/*" org.main.Main --processType publish --operationType produce_data

<h5>Tumbling Windows With Processing Time Notion</h5>
java -cp "<jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar:<flink-library-path>/flink-1.5.0/lib/*" org.main.Main --processType publish --operationType produce_data
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType streaming --operationType tumbling_window_processing_time_notion

<h5>Produce timestamp data into the socket for Event Time Notion</h5>
we need flink libs at classpath because we are embedding flink code as well
java -cp "<jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar:<flink-library-path>/flink-1.5.0/lib/*" org.main.Main --processType publish --operationType produce_timestamp_data

<h5>Tumbling Windows With Event Time Notion</h5>
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType streaming --operationType tumbling_window_event_time_notion

<h5>Sliding Windows With Processing Time Notion</h5>
java -cp "<jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar:<flink-library-path>/flink-1.5.0/lib/*" org.main.Main --processType publish --operationType produce_data
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType streaming --operationType sliding_window_processing_time_notion


<h5>Session Windows With Processing Time Notion</h5>
java -cp "<jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar:<flink-library-path>/flink-1.5.0/lib/*" org.main.Main --processType publish --operationType produce_data
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType streaming --operationType session_window_processing_time_notion


<h5>Global Windows With Processing Time Notion</h5>
java -cp "<jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar:<flink-library-path>/flink-1.5.0/lib/*" org.main.Main --processType publish --operationType produce_data
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType streaming --operationType global_window_processing_time_notion


<h5>Value State Example</h5>
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType streaming --operationType value_state_example


<h5>Checkpointing Example</h5>
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType streaming --operationType checkpointing_example

<h5>Settings to consume data from x</h5>
   - go to : apps.twitter.com
   - create developer account
   - navigate to keys and tokens tab
   - Api Key: 
   - Api Secret: 
   - Access Token: 
   - Access Token Secret: 

<h5>Consumer X Data Example</h5>
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType streaming --operationType consume_from_kafka

<h5>Real time stock analysis</h5>
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType streaming --operationType run_stock_analysis

<h5>Table Api Example</h5>
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType batch --operationType table_api_example 

<h5>SQL Api Example</h5>
./bin/flink run <jar-path>/apache-flink-1.0-SNAPSHOT-jar-with-dependencies.jar --processType batch --operationType sql_api_example 
