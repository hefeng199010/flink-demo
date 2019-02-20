package com.flink.finkdemo;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

public class MessageSplitter implements FlatMapFunction<String, Tuple2<String, Long>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
        if (value != null && value.contains(",")) {
            String[] parts = value.split(",");
            out.collect(new Tuple2<>(parts[1], Long.parseLong(parts[2])));
        }
    }
}
