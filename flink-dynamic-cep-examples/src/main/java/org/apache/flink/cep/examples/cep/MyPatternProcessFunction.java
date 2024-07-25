package org.apache.flink.cep.examples.cep;

import org.apache.flink.cep.functions.AbstractPatternProcessFunction;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * 对匹配到的数据取平均值，并输出阈值
 *
 * @author shirukai
 */
public class MyPatternProcessFunction extends AbstractPatternProcessFunction<Row, Row> {
    private static final ConfigOption<Double> TEMP_THRESHOLD = ConfigOptions.key("temp_threshold")
            .doubleType().defaultValue(100.0);

    private static final ConfigOption<Double> RPM_THRESHOLD = ConfigOptions.key("rpm_threshold")
            .doubleType().defaultValue(6000.0);

    private  double tempThreshold;
    private  double rpmThreshold;

    @Override
    public void open(Configuration parameters) throws Exception {
        tempThreshold = parameters.get(TEMP_THRESHOLD);
        rpmThreshold = parameters.get(RPM_THRESHOLD);
    }

    @Override
    public void processMatch(Map<String, List<Row>> match, PatternProcessFunction.Context ctx, Collector<Row> out) throws Exception {
        List<Row> events = match.get("start");
        // 1. 计算均值
        double rpmAvg = events.stream().mapToLong(row -> row.getFieldAs("rpm")).average().orElse(0.0);
        double tempAvg = events.stream().mapToDouble(row -> row.getFieldAs("temp")).average().orElse(0.0);
        // 2. 输出结果
        out.collect(Row.of(events.get(0).getFieldAs("id"), rpmAvg, tempAvg, events.get(0).getFieldAs("detection_time"), rpmThreshold, tempThreshold));
    }
}