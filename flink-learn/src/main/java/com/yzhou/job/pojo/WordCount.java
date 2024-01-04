package com.yzhou.job.pojo;

import lombok.Data;

@Data
public class WordCount {
    //单词
    private String word;

    //每个单词数量
    private Integer count;

    public WordCount() {
    }

    public WordCount(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
