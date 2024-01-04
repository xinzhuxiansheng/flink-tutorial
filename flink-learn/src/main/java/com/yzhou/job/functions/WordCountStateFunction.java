package com.yzhou.job.functions;

import com.yzhou.job.pojo.WordCount;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class WordCountStateFunction extends RichFlatMapFunction<WordCount, WordCount> {

    //状态变量
    /* **********************
     *
     * KeyedState的数据类型有：
     * ValueState<T>: 状态的数据类型为单个值，这个值是类型T
     * ListState<T>: 状态的数据类型为列表，列表值是类型T
     *
     * *********************/
    private ValueState<WordCount> keyedState;

    @Override
    public void open(Configuration parameters) throws Exception {

        ValueStateDescriptor<WordCount> valueStateDescriptor =
                // valueState描述器
                new ValueStateDescriptor<WordCount>(
                        // 描述器的名称
                        "wordcountstate",
                        //描述器的数据类型
                        /* **********************
                         *
                         * 知识点：
                         *
                         * Flink有自己的一套数据类型，包含了JAVA和Scala的所有数据类型
                         * 这些数据类型都是TypeInformation对象的子类。
                         * TypeInformation对象统一了所有数据类型的序列化实现
                         *
                         * *********************/
                        TypeInformation.of(WordCount.class)
                );

        keyedState = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void flatMap(WordCount input, Collector<WordCount> output) throws Exception {

        //读取状态
        WordCount lastKeyedState = keyedState.value();

        //更新状态
        if (lastKeyedState == null) {
            //状态还未赋值的情况

            //更新状态
            keyedState.update(input);
            //返回原数据
            output.collect(input);
        } else {
            //状态存在旧的状态数据的情况
            Integer count = lastKeyedState.getCount() + input.getCount();
            WordCount newWordCount = new WordCount(input.getWord(), count);

            //更新状态
            keyedState.update(newWordCount);
            //返回新的数据
            output.collect(newWordCount);
        }
    }
}
