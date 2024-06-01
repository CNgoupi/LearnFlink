package org.ai88.bigdata.wc;

import javassist.bytecode.analysis.Type;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: goupi
 * @date 2024/5/31 23:06
 * @description : 流式计算 注意，stream的分组是keyBy，批处理是groupBy
 */
public class WordCountDemo2 {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountDemo2.class);
    public static void main(String[] args) {
        try {
            // 环境搭建
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // 读取数据
            DataStreamSource<String> ds = env.socketTextStream("hadoop001", 9999);

            // 数据转换
            ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>(){

                @Override
                public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                    String[] split = value.split(" ");
                    for (String s : split) {
                        Tuple2<String,Integer> t = new Tuple2<>(s,1);
                        out.collect(t);
                    }
                }
            }).keyBy(new KeySelector<Tuple2<String, Integer>, String>(){

                @Override
                public String getKey(Tuple2<String, Integer> value) throws Exception {
                    return value.f0;
                }
            }).sum(1).print();

            // 执行
            env.execute("WordCountDemo2");
        }catch (Exception e){
            LOG.error(e.getMessage(), e);
        }
    }
}
