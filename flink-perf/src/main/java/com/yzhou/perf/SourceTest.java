package com.yzhou.perf;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class SourceTest extends RichParallelSourceFunction<String> {

    private final static Logger logger = LoggerFactory.getLogger(SourceTest.class);
    private static final String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";


    private int max;
    private int recordSize;
    private long counter = 0L;
    private Random random;
    private Semaphore semaphore = null;
    private Map<String,Object> commandLine;

    public SourceTest(Map<String,Object> commandLine){
        this.commandLine = commandLine;
    }

    public SourceTest() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if(commandLine.containsKey("singleTaskQPS")){
            max = (int) commandLine.get("singleTaskQPS");
        }
        if(commandLine.containsKey("recordSize")){
            recordSize = (int) commandLine.get("recordSize");
        }
        random = new Random();
        semaphore = new Semaphore(max);

        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                semaphore.release(max);
                logger.info("couter {}", counter);
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    public String getRandomString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (true) {
            try {
                semaphore.acquireUninterruptibly(1);
                counter++;
                if (sourceContext != null) {
                    sourceContext.collect(getRandomString(recordSize));
                }
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public void cancel() {

    }
}
