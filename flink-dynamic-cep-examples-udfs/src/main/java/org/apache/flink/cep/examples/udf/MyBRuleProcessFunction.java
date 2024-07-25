package org.apache.flink.cep.examples.udf;


import lombok.Data;
import org.apache.flink.cep.context.RuleFunctionContext;
import org.apache.flink.cep.functions.AbstractRuleProcessFunction;
import org.apache.flink.cep.types.FieldRow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 对输入数据进行不同逻辑的处理，B设备的数据进行求和
 *
 * @author shirukai
 */
public class MyBRuleProcessFunction extends AbstractRuleProcessFunction<String, Row, FieldRow> {


    @Override
    public void process(Row value, RuleFunctionContext<String> ctx, Collector<FieldRow> out) throws Exception {
        Long currentRpm = value.<Long>getFieldAs("rpm");
        Double currentTemp = value.<Double>getFieldAs("temp");
        // 1. 从Flink获取自定义状态
        MyState myState = ctx.<MyState>getState().orElse(new MyState());

        // 2. 求和
        myState.rpmSum += currentRpm;
        myState.tempSum += currentTemp;

        // 3. 输出计算结果


        Map<String, Object> data = new HashMap<>();
        data.put("rpm_sum", myState.rpmSum);
        data.put("temp_sum", myState.tempSum);
        out.collect(FieldRow.of(2).add("id",value.getFieldAs("id")).add("data",data));

        // 4.自定义的状态在操作完成后，需要将状态更新至Flink
        ctx.updateState(myState);
    }

    @Data
    public static class MyState implements Serializable {
        private Double tempSum = 0.0;
        private Long rpmSum = 0L;
    }
}
