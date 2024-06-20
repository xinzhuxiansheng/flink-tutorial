package com.yzhou.blog.wordcount.checkpoint;


public class KeyedState {
    public String key;

    public Long value;

    @Override
    public String toString() {
        return "KeyedState{" +
                "key='" + key + '\'' +
                ", value=" + value +
                '}';
    }
}

