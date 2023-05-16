package com.sqs.app.func;

import com.sqs.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {
    public void eval(String text) {
        try {
            for (String keyword : KeyWordUtil.splitKeyword(text)) {
                collect(Row.of(keyword));
            }
        } catch (IOException e) {
            collect(Row.of(text));
        }
    }
}
