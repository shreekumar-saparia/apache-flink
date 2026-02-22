package org.sql_api;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class SQLApiExample {
    public static void sqlApiExample(ExecutionEnvironment env) throws Exception {
        //env = ExecutionEnvironment.getExecutionEnvironment();
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

        String sql = "SELECT `month`, SUM(profit) AS sum1 FROM CatalogTable WHERE category = 'Category5'"
                + " GROUP BY `month` ORDER BY sum1";
        Table order20 = tableEnv.sqlQuery(sql);

        DataSet<Row1> order20Set = tableEnv.toDataSet(order20, Row1.class);

        order20Set.writeAsText("<output-path>/output/sql_api_op");
        env.execute("SQL API Example");
    }

    public static class Row1
    {
        public String month;
        public Integer sum1;

        public Row1(){}

        public String toString()
        {
            return month + "," + sum1;
        }
    }
}
