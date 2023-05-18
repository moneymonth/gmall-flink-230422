package com.sqs.utils;

//import com.sqs.bean.KeywordBean;
//import com.sqs.bean.TransientSink;
//import com.sqs.common.GmallConfig;
//import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
//import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
//import org.apache.flink.connector.jdbc.JdbcSink;
//import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//
//import java.lang.reflect.Field;
//import java.lang.reflect.Method;
//import java.sql.PreparedStatement;
//import java.sql.SQLException;
//
//public class MyClickHouseUtil {
//    public static <T> SinkFunction<T> getSinkFunction(String sql) {
//
//        return JdbcSink.<T>sink(sql, new JdbcStatementBuilder<T>() {
//                    @Override
//                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
//
////                        反射获取t对象得属性
//                        Class<?> tClz = t.getClass();
////                        Method[] methods = tClz.getMethods();
////                        for (int i = 0; i < methods.length; i++) {
////                            Method method = methods[i];
////                            method.invoke(t);
////                        }
//
//                        Field[] declaredFields = tClz.getDeclaredFields();
//
//                        for (int i = 0; i < declaredFields.length; i++) {
//                            int offset=0;
//                            Field field = declaredFields[i];
//                            field.setAccessible(true);
//
//                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
//                            if (transientSink !=null){
//                                offset++;
//                                continue;
//                            }
//
//                            try {
//                                Object o = field.get(t);
//                                preparedStatement.setObject(i + 1 - offset, o);
//                            } catch (IllegalAccessException e) {
//                                throw new RuntimeException(e);
//                            }
//
//                        }
//                    }
//                }, new JdbcExecutionOptions.Builder()
//                        .withBatchSize(5)
//                        .withBatchIntervalMs(1000L)
//                        .build(),
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
//                        .withUrl(GmallConfig.CLICKHOUSE_URL)
//                        .build());
//    }
//}
import com.sqs.bean.KeywordBean;
import com.sqs.bean.TransientSink;
import com.sqs.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyClickHouseUtil {

    public static <T> SinkFunction<T> getSinkFunction(String sql) {
        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        //使用反射的方式获取t对象中的数据
                        Class<?> tClz = t.getClass();

//                        Method[] methods = tClz.getMethods();
//                        for (int i = 0; i < methods.length; i++) {
//                            Method method = methods[i];
//                            method.invoke(t);
//                        }

                        //获取并遍历属性
                        Field[] declaredFields = tClz.getDeclaredFields();
                        int offset = 0;
                        for (int i = 0; i < declaredFields.length; i++) {

                            //获取单个属性
                            Field field = declaredFields[i];
                            field.setAccessible(true);

                            //尝试获取字段上的自定义注解
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                offset++;
                                continue;
                            }

                            //获取属性值
                            Object value = field.get(t);

                            //给占位符赋值
                            preparedStatement.setObject(i + 1 - offset, value);

                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }

}
