package org.apache.flink.cep.examples.cep;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.cep.functions.AbstractCondition;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * 接受"start"模式下，一个事件id等于device-1，并且已经匹配事件的温度或转速均值大于制定阈值
 *
 * @author shirukai
 */
public class MyCondition extends AbstractCondition<Row> {
    private static final ConfigOption<Double> TEMP_THRESHOLD = ConfigOptions.key("temp_threshold")
            .doubleType().defaultValue(100.0);

    private static final ConfigOption<Double> RPM_THRESHOLD = ConfigOptions.key("rpm_threshold")
            .doubleType().defaultValue(6000.0);
    private  double tempThreshold;
    private  double rpmThreshold;

    @Override
    public void open(Configuration configuration) throws Exception {
        tempThreshold = configuration.get(TEMP_THRESHOLD);
        rpmThreshold = configuration.get(RPM_THRESHOLD);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean filter(Row value, IterativeCondition.Context<Row> ctx) throws Exception {
        if (!"device-1".equals(value.getFieldAs("id"))) {
            return false;
        }
        List<Row> events = (List<Row>) IteratorUtils.toList(ctx.getEventsForPattern("start").iterator());
        events.add(value);

        // 1. 计算转速平均值
        double rpmAvg = events.stream().mapToLong(row -> row.getFieldAs("rpm")).average()
                .orElse(value.<Long>getFieldAs("rpm").doubleValue());

        // 2. 计算温度平均值
        double tempAvg = events.stream().mapToDouble(row -> row.getFieldAs("temp")).average()
                .orElse(value.<Double>getFieldAs("temp"));


        // 3. 判断转速均值或者温度均值是否超出阈值
        return rpmAvg > rpmThreshold || tempAvg > tempThreshold;
    }
}
