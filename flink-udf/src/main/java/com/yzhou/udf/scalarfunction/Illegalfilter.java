package com.yzhou.udf.scalarfunction;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.math.BigDecimal;

/*
    拼接 两个字段
 */
public class Illegalfilter extends ScalarFunction {
    public Illegalfilter() {}
    public String eval(
            String FK_SAACN_KEY,
            @DataTypeHint("DECIMAL(20, 2)")BigDecimal SA_TX_AMT,
            @DataTypeHint("DECIMAL(20, 2)") BigDecimal SA_DR_AMT) {
        return "1";
    }
}
