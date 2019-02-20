package com.flink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * wordcount
 */
public class FlinkSocketApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines= env.socketTextStream("172.16.200.114",10000,"\n");
        DataStream<WordWithCount> windowCounts =lines.flatMap(new FlatMapFunction<String, WordWithCount>() {

            @Override
            public void flatMap(String value, Collector<WordWithCount> collector) throws Exception {
                for(String word:value.split(" ")){
                    collector.collect(new WordWithCount(word,1));
                }
            }
        })
                .keyBy("word")
                .timeWindow(Time.seconds(4),Time.seconds(2))
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });
        windowCounts.print().setParallelism(2);
        env.execute("aaaaaaa");
}}
