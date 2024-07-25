package org.apache.flink.cep.examples.cep;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEPUtils;
import org.apache.flink.cep.TimeBehaviour;
import org.apache.flink.cep.discover.JdbcPeriodicRuleDiscovererFactory;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Collections;

/**
 * Flink CEP 引擎支持动态多规则示例
 *
 * @author shirukai
 */
public class DynamicCepExamples {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Row> events = env.fromElements(
                        Row.of("device-1", 50.0, 5600L, 1705307073000L),
                        Row.of("device-1", 60.8, 6670L, 1705307085000L),
                        Row.of("device-1", 56.4, 6430L, 1705307040000L),
                        Row.of("device-1", 60.8, 6670L, 1705307205000L)
                )
                .returns(Types.ROW_NAMED(new String[]{"id", "temp", "rpm", "detection_time"}, Types.STRING, Types.DOUBLE, Types.LONG, Types.LONG));


        SingleOutputStreamOperator<Row> alarms = CEPUtils.dynamicCepRules(
                events.keyBy((KeySelector<Row, String>) value -> value.getFieldAs("id")),
                new JdbcPeriodicRuleDiscovererFactory(
                        JdbcConnectorOptions.builder()
                                .setTableName("public.cep_rules")
                                .setDriverName("org.postgresql.Driver")
                                .setDBUrl("jdbc:postgresql://20.5.2.35:5432/postgres")
                                .setUsername("postgres")
                                .setPassword("hollysys")
                                .build(),
                        3,
                        "cep",
                        Collections.emptyList(),
                        Duration.ofMinutes(1).toMillis()),
                TimeBehaviour.ProcessingTime,
                Types.ROW_NAMED(new String[]{"id", "rpm_avg", "temp_avg", "detection_time", "rpm_threshold", "temp_threshold"},
                        Types.STRING, Types.DOUBLE, Types.DOUBLE, Types.LONG, Types.DOUBLE, Types.DOUBLE),
                "cep-test",
                "/",
                false
        );

        alarms.print();

        env.execute("DynamicCepExamples");

    }

}
