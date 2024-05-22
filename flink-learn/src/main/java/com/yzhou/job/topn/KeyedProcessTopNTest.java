package com.yzhou.job.topn;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.*;

public class KeyedProcessTopNTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        // 从自定义数据源读取数据
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        //需要按照url分组，求出每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> urlCountStream =
                eventStream.keyBy(data -> data.url)
                        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                        //这里为什么不用reduce 因为之前是Event 我们想要UrlViewCount
                        //这里为什么不用process 因为process效率比较低。需要一起处理
                        .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        SingleOutputStreamOperator<String> result = urlCountStream.keyBy(data -> data.windowEnd)
                .process(new TopN(2));

        result.print("result");

        //process处理
        SingleOutputStreamOperator<String> process = eventStream
                .keyBy(data -> true) //这里是划分到一个并行度了
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new MyProcess());
        process.print("process");
        //这么写也可以和上面的process一样
//        eventStream
//                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//                .process(new MyProcess2());
        // 对结果中同一个窗口的统计数据，进行排序处理

        env.execute();
    }

    // 自定义增量聚合
    public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    // 自定义全窗口函数，只需要包装窗口信息
    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            // 结合窗口信息，包装输出内容
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
        }
    }

    // 自定义处理函数，排序取top n
    public static class TopN extends KeyedProcessFunction<Long, UrlViewCount, String> {
        // 将n作为属性
        private Integer n;
        // 定义一个列表状态 //为什么这里不直接赋值，因为还没有获取到上下文环境
        private ListState<UrlViewCount> urlViewCountListState;

        public TopN(Integer n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 从环境中获取列表状态句柄
            urlViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("url-view-count-list", Types.POJO(UrlViewCount.class)));
        }

        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 将count数据添加到列表状态中，保存起来
            urlViewCountListState.add(value);
            // 注册 window end + 1ms后的定时器，等待所有数据到齐开始排序
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 将数据从列表状态变量中取出，放入ArrayList，方便排序
            ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();
            for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
                urlViewCountArrayList.add(urlViewCount);
            }
            // 清空状态，释放资源
            urlViewCountListState.clear();

            // 排序
            urlViewCountArrayList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });

            // 取前两名，构建输出结果
            StringBuilder result = new StringBuilder();
            result.append("========================================\n");
            result.append("窗口结束时间：" + new Timestamp(timestamp - 1) + "\n");
            for (int i = 0; i < this.n; i++) {
                UrlViewCount UrlViewCount = urlViewCountArrayList.get(i);
                String info = "No." + (i + 1) + " "
                        + "url：" + UrlViewCount.url + " "
                        + "浏览量：" + UrlViewCount.count + "\n";
                result.append(info);
            }
            result.append("========================================\n");
            out.collect(result.toString());
        }
    }

    public static class MyProcess extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean s, Context context, Iterable<Event> iterable, Collector<String> collector) throws Exception {
            HashMap<String, Long> urlCountMap = new HashMap<>();
            // 遍历窗口中数据，将浏览量保存到一个 HashMap 中
            for (Event event : iterable) {
                String url = event.url;
                if (urlCountMap.containsKey(url)) {
                    long count = urlCountMap.get(url);
                    urlCountMap.put(url, count + 1L);
                } else {
                    urlCountMap.put(url, 1L);
                }
            }
            ArrayList<Tuple2<String, Long>> mapList = new ArrayList<Tuple2<String, Long>>();
            // 将浏览量数据放入ArrayList，进行排序
            for (String key : urlCountMap.keySet()) {
                mapList.add(Tuple2.of(key, urlCountMap.get(key)));
            }
            mapList.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    return o2.f1.intValue() - o1.f1.intValue();
                }
            });
            // 取排序后的前两名，构建输出结果
            StringBuilder result = new StringBuilder();
            System.out.println(urlCountMap);
            result.append("========================================\n");
            int size = urlCountMap.size();
            int top = Integer.min(size, 2);
            for (int i = 0; i < top; i++) {
                Tuple2<String, Long> temp = mapList.get(i);
                String info = "浏览量No." + (i + 1) +
                        " url：" + temp.f0 +
                        " 浏览量：" + temp.f1 +
                        " 窗口结束时间：" + new Timestamp(context.window().getEnd()) + "\n";

                result.append(info);
            }
            result.append("========================================\n");
            collector.collect(result.toString());
        }
    }

    public static class ClickSource implements SourceFunction<Event> {
        // 声明一个布尔变量，作为控制数据生成的标识位
        private Boolean running = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            Random random = new Random();    // 在指定的数据集中随机选取数据
            String[] users = {"Mary", "Alice", "Bob", "Cary"};
            String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

            while (running) {
                ctx.collect(new Event(
                        users[random.nextInt(users.length)],
                        urls[random.nextInt(urls.length)],
                        Calendar.getInstance().getTimeInMillis()
                ));
                //这里也可以发送水位线
                //           ctx.collectWithTimestamp();
//            ctx.emitWatermark(new Watermark(1));
                // 隔1秒生成一个点击事件，方便观测
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

    }

    public static class Event {
        public String user;
        public String url;
        public Long timestamp;

        public Event() {
        }

        public Event(String user, String url, Long timestamp) {
            this.user = user;
            this.url = url;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "user='" + user + '\'' +
                    ", url='" + url + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
        }
    }
    public static class UrlViewCount {
        public String url;
        public Long count;
        public Long windowStart;
        public Long windowEnd;
        public UrlViewCount() {
        }
        public UrlViewCount(String url, Long count, Long windowStart, Long windowEnd) {
            this.url = url;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }
        @Override
        public String toString() {
            return "UrlViewCount{" +
                    "url='" + url + '\'' +
                    ", count=" + count +
                    ", windowStart=" + new Timestamp(windowStart) +
                    ", windowEnd=" + new Timestamp(windowEnd) +
                    '}';
        }
    }
}
