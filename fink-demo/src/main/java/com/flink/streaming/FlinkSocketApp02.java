package com.flink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * wordcount
 */
public class FlinkSocketApp02 {
    public static void main(String[] args) throws Exception{

        final String hostname;
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.get("hostname","172.16.200.114");
            port = params.getInt("port",10000);
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount " +
                    "--hostname <hostname> --port <port>', where hostname (localhost by default) " +
                    "and port is the address of the text server");
            System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
                    "type the input text into the command line");
            return;
        }
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines= env.socketTextStream(hostname,port,"\n");
        DataStream<Tuple2<String,Integer>> windowCounts =lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words=s.split(" ");
                for(String word : words){
                    collector.collect(new Tuple2<>(word,1));
                }
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(4),Time.seconds(2))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                        return new Tuple2<>(t1.f0,t1.f1+t2.f1);
                    }
                });

        windowCounts.print().setParallelism(2);
        env.execute("FlinkSocketApp02");
}}
