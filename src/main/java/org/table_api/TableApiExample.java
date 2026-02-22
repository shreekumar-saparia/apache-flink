package org.table_api;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class TableApiExample {

    public static void tableApiExample(ExecutionEnvironment env) throws Exception {
        env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        /* create table from csv */
        TableSource tableSrc = CsvTableSource.builder()
                .path("<input-path>/avg")
                .fieldDelimiter(",")
                .field("date", Types.STRING)
                .field("month", Types.STRING)
                .field("category", Types.STRING)
                .field("product", Types.STRING)
                .field("profit", Types.INT)
                .build();

        tableEnv.registerTableSource("CatalogTable", tableSrc);

        Table catalog = tableEnv.scan("CatalogTable");

        /* querying with Table API */
        Table order20 = catalog
                .filter(" category === 'Category5'")
                .groupBy("month")
                .select("month, profit.sum as sum")
                .orderBy("sum");


        DataSet<Row1> order20Set = tableEnv.toDataSet(order20, Row1.class);

        order20Set.writeAsText("<output-path>/output/table_api_op");
        //tableEnv.toAppendStream(order20, Row.class).writeAsText("<output-path>/output/table_api_op");
        env.execute("Table Api Example");
    }

    public static class Row1 {
        public String month;
        public Integer sum;

        public Row1() {
        }

        public String toString() {
            return month + "," + sum;
        }
    }
}
