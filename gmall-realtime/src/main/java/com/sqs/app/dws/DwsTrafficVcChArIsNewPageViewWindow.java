package com.sqs.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sqs.bean.TrafficPageViewBean;
import com.sqs.utils.DateFormatUtil;
import com.sqs.utils.MyClickHouseUtil;
import com.sqs.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        TODO 读取三个主题创建流
        String topic = "dwd_traffic_page_log";
        String ujdTopic = "dwd_traffic_user_jump_detail";
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        String groupId= "vccharisnew_pageview_window";
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(uvTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(ujdTopic, groupId));
        DataStreamSource<String> pageDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
//        TODO 统一数据格式
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUvDS = uvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"), 1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts"));
        });
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUjDS = ujDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 0L, 1L,
                    jsonObject.getLong("ts"));
        });

        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithPageDS = pageDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");
            String lastPageId = page.getString("last_page_id");
            long sv = 0L;
            if (lastPageId == null) {
                sv = 1L;
            }
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, sv, 1L, page.getLong("during_time"), 0L,
                    jsonObject.getLong("ts"));
        });
//        TODO 将三个流进行Union
        DataStream<TrafficPageViewBean> unionDS = trafficPageViewWithUvDS.union(
                trafficPageViewWithUjDS,
                trafficPageViewWithPageDS
        );
//        TODO 提取事件事件生产watermark
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithWmDS
                = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(
                Duration.ofSeconds(14)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                return trafficPageViewBean.getTs();
            }
        }));
//        TODO 分组开窗聚合
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowedStream = trafficPageViewWithWmDS.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean trafficPageViewBean) throws Exception {
                return new Tuple4<>(
                        trafficPageViewBean.getAr(),
                        trafficPageViewBean.getIsNew(),
                        trafficPageViewBean.getCh(),
                        trafficPageViewBean.getVc()
                );
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)));

//        增量
//        windowedStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
//            @Override
//            public TrafficPageViewBean reduce(TrafficPageViewBean trafficPageViewBean, TrafficPageViewBean t1) throws Exception {
//                return null;
//            }
//        });
//        全量

        SingleOutputStreamOperator<Object> resultDS = windowedStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean trafficPageViewBean, TrafficPageViewBean t1) throws Exception {
                trafficPageViewBean.setSvCt(trafficPageViewBean.getSvCt() + t1.getSvCt());
                trafficPageViewBean.setUvCt(trafficPageViewBean.getUvCt() + t1.getUvCt());
                trafficPageViewBean.setUjCt(trafficPageViewBean.getUjCt() + t1.getUjCt());
                trafficPageViewBean.setPvCt(trafficPageViewBean.getPvCt() + t1.getPvCt());
                trafficPageViewBean.setDurSum(trafficPageViewBean.getDurSum() + t1.getDurSum());
                return trafficPageViewBean;
            }
        }, new WindowFunction<TrafficPageViewBean, Object, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> key, TimeWindow timeWindow, Iterable<TrafficPageViewBean> iterable, Collector<Object> collector) throws Exception {

//                获取数据
                TrafficPageViewBean next = iterable.iterator().next();
//                补充信息
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
//                修改TS

                next.setTs(System.currentTimeMillis());

//                输出数据
                collector.collect(next);
            }
        });


//        TODO 将数据写出到ClickHouse
        resultDS.print(">>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"));
//        TODO 启动任务
        env.execute("DwsTrafficVcChArIsNewPageViewWindow");

    }
}
