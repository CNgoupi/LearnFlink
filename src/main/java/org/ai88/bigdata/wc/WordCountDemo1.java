package org.ai88.bigdata.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author: goupi
 * @date 2024/5/18 17:38
 * DataSet API 不推荐
 */
public class WordCountDemo1 {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.读取文件数据 按行读取数据 生成数据集
        DataSource<String> ds = env.readTextFile("input/test.txt");

        // 3.数据处理 生成二元组(word,1)
        /**
         * 在这里拆成二元组的话，因为传进来的是一行数据，一行数据
         * 是有多个单词的。所以这里拆分成二元组的话。一行数据是会有
         * 多个二元组生成的。因此这里用flatMap()。
         * map的话只能生成一个二元组。这里不用map()
         *
         * flatMap()怎么使用呢？点击进入flatMap的源码
         * public <R> FlatMapOperator<T, R> flatMap(FlatMapFunction<T, R> flatMapper)
         * 它的参数是一个类型。继续点击进入到FlatMapFunction里面
         * 发现FlatMapFunction是一个接口类型public interface FlatMapFunction<T, O> extends Function, Serializable
         * 这个类型上面是解释：
         <T> – Type of the input elements. 输入的数据类型
         <O> – Type of the returned elements. 输出的类型
         这两个是我们自己去指定
         由于FlatMapFunction是一个接口，所以我们可以自己造一个类来实现接口
         或者使用匿名类的方式。由于我们这里没有实际的业务，所以这里使用匿名的类的方式
         去实现接口的方法。
         */
        FlatMapOperator<String, Tuple2<String, Integer>> wordOneDs = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 3.1 一行数据拆分成单词
                String[] words = value.split(" ");
                // 3.2 生成二元组
                for (String word : words) {
                    // out是采集器 向下传输的
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });

        // 4.分组
        UnsortedGrouping<Tuple2<String, Integer>> wordOneGroup = wordOneDs.groupBy(0);

        // 5.聚合
        AggregateOperator<Tuple2<String, Integer>> sum = wordOneGroup.sum(1);

        // 6.输出
        sum.print();
    }
}
