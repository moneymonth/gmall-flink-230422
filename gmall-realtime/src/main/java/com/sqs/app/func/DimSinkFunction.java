package com.sqs.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.sqs.utils.DruidDSUtil;
import com.sqs.utils.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.SQLException;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
  private DruidDataSource druidDataSource =null;
    @Override
    public void open(Configuration parameters) throws Exception {
      druidDataSource=  DruidDSUtil.createDataSource();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        DruidPooledConnection connection=druidDataSource.getConnection();

        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");


            PhoenixUtil.upsertValues(connection,sinkTable,data);



        connection.close();
    }
}
