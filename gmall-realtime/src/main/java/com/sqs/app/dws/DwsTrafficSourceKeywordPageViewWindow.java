package com.sqs.app.dws;

import com.sqs.app.func.SplitFunction;
import com.sqs.bean.KeywordBean;
import com.sqs.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        TODO 使用DDL方式读取kafka page_log 数据创建表并提取时间生产Watermark
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_source_keyword_page_view_window";
        tableEnv.executeSql(""+
                "create table page_log( " +
                "    `page` map<string,string>,  " +
                "    `ts` bigint,  " +
                "    `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),  " +
                "    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND  " +
                ") "+ MyKafkaUtil.getKafkaDDL(topic,groupId));
//        TODO 过滤搜索数据
        Table filterTable = tableEnv.sqlQuery("" +
                "select  " +
                "    page['item'] item,  " +
                "    rt  " +
                "from page_log  " +
                "where page['last_page_id'] = 'search'  " +
                "and page['item_type'] = 'keyword'  " +
                "and page['item'] is not null");
        tableEnv.createTemporaryView("filter_table",filterTable);
//        TODO 注册UDTF & 切词
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);


        Table splitTable = tableEnv.sqlQuery("" +
                "SELECT  " +
                "    word,  " +
                "    rt  " +
                "FROM filter_table,   " +
                "LATERAL TABLE(SplitFunction(item))");
        tableEnv.createTemporaryView("split_table",splitTable);


//        TODO 分组，开窗，聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    'search' source, " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "    word keyword, " +
                "    count(*) keyword_count, " +
                "    UNIX_TIMESTAMP()*1000 ts " +
                "from split_table " +
                "group by word,TUMBLE(rt, INTERVAL '10' SECOND)");
//        TODO 将动态表转为流

        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);
        keywordBeanDataStream.print(">>>>>>>>>>>>");
//        TODO 将数据写出CK
//        TODO 启动任务
        env.execute("DwsTrafficSourceKeywordPageViewWindow");


    }
}
