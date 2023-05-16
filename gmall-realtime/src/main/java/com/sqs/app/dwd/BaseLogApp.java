package com.sqs.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sqs.utils.DateFormatUtil;
import com.sqs.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
//        1获取执行环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


//        2消费kafka 数据 创建流

        String topic = "topic_log";
        String groupId = "Base_Log_App";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

//        3过滤掉非json的数据 ， 将每行数据转为json对象

        OutputTag<String> dirtyStreamTag  = new OutputTag<String>("Dirty"){
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyStreamTag, s);
                }
            }
        });

        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyStreamTag);
        dirtyDS.print("Dirty>>>>>>>>>>>>>>>>>>>>>>");


//        4按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

//        5 使用状态编码做新老访客标记校验
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-visit", String.class));

            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {

//                获取is_new标记 和 ts 转换成年月日
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                Long ts = jsonObject.getLong("ts");
                String curDate = DateFormatUtil.toDate(ts);
//                获取状态中的日期
                String lastDate = lastVisitState.value();
//                判断is_new是否为“1”

                if ("1".equals(isNew)) {
                    if (lastDate == null) {
                        lastVisitState.update(curDate);
                    } else if (!lastDate.equals(curDate)) {
                        jsonObject.getJSONObject("common").put("is_new", "0");
                    }
                } else if (lastDate == null) {

                    lastVisitState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));
                }
                return jsonObject;
            }
        });
//        6 使用侧输出流进行分流 页面日志放主流
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("action"){};
        OutputTag<String> errorTag= new OutputTag<String>("error"){};
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {


            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
//              获取错误信息
                String err = jsonObject.getString("err");
                if (err != null) {
//                    将错误信息写入侧输出流
                    context.output(errorTag, jsonObject.toJSONString());
                }
//                  移除错误信息
                jsonObject.remove("err");
//                获取启动信息
                String start = jsonObject.getString("start");
                if (start != null) {
//                将数据写道start测输出流
                    context.output(startTag, jsonObject.toJSONString());
                } else {
                    String common = jsonObject.getString("common");
                    String pageId = jsonObject.getJSONObject("page").getString("page_id");
                    Long ts = jsonObject.getLong("ts");

//                    获取曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject displaysJSONObject = displays.getJSONObject(i);
                            displaysJSONObject.put("common", common);
                            displaysJSONObject.put("page_id", pageId);
                            displaysJSONObject.put("ts", ts);
                            context.output(displayTag, displaysJSONObject.toJSONString());
                        }
                    }

                    //                    获取动作数据
                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("page_id", pageId);

                            context.output(actionTag, action.toJSONString());
                        }
                    }

                    jsonObject.remove("displays");
                    jsonObject.remove("actions");
                    collector.collect(jsonObject.toJSONString());
                }
            }
        });
//        7 提取各个测输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);
//        8 将数据打印写入对于的kafka 主题
        pageDS.print("Page>>>>>>>>>>>>");
        startDS.print("Start>>>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>>>>");
        actionDS.print("Action>>>>>>>>>>>>");
        errorDS.print("Error>>>>>>>>>>>>");

        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(start_topic));
        dirtyDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(action_topic));
        errorDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(error_topic));
//        9 启动任务

        env.execute("BaseLogApp");

    }
}
