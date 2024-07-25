package org.apache.flink.cep.examples.udf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEPUtils;
import org.apache.flink.cep.discover.JdbcPeriodicRuleDiscovererFactory;
import org.apache.flink.cep.types.FieldRow;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowUtils;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

/**
 * 自定义规则引擎支持动态规则示例
 *
 * @author shirukai
 */
public class DynamicUdfExamples {
    public static void main(String[] args) throws Exception {


        String jobId = "f35242b392d48e387b5fa6d321672e51";
        String checkpointDir = "file:///Users/shirukai/tmp/flink-checkpoints";
        Configuration configuration = new Configuration();
        configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId);
        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
        configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);

        File dirFile = Paths.get(URI.create(checkpointDir).getPath(), jobId).toFile();
        if (dirFile.exists()) {
            String[] dirs = Arrays.stream(Objects.requireNonNull(dirFile.listFiles()))
                    .filter(File::isDirectory)
                    .filter(file -> file.getName().startsWith("chk-"))
                    .sorted((o1, o2) -> Long.compare(o2.lastModified(), o1.lastModified()))
                    .map(File::getName)
                    .toArray(String[]::new);

            if (dirs.length > 0) {
                String savepointPath = Paths.get(dirFile.getAbsolutePath(), dirs[dirs.length - 1]).toString();
                configuration.set(SavepointConfigOptions.SAVEPOINT_PATH, savepointPath);
            }
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        DataStream<Row> events =  env.socketTextStream("localhost",15555)
                .map(s->{
                    String[] items = s.split(",");
                    return Row.of(items[0],Double.valueOf(items[1]),Long.valueOf(items[2]),Long.valueOf(items[3]));
                }).returns(Types.ROW_NAMED(new String[]{"id", "temp", "rpm", "detection_time"}, Types.STRING, Types.DOUBLE, Types.LONG, Types.LONG));



//        DataStream<Row> events = env.fromElements(
//                        Row.of("device-1", 50.0, 5600L, 1705307073000L),
//                        Row.of("device-2", 60.8, 6670L, 1705307085000L),
//                        Row.of("device-1", 56.4, 6430L, 1705307040000L),
//                        Row.of("device-2", 60.8, 6670L, 1705307205000L)
//                )
//                .returns(Types.ROW_NAMED(new String[]{"id", "temp", "rpm", "detection_time"}, Types.STRING, Types.DOUBLE, Types.LONG, Types.LONG));
        SingleOutputStreamOperator<FieldRow> alarms = CEPUtils.dynamicUdfRules(
                events.keyBy((KeySelector<Row, String>) value -> value.getFieldAs(0)),
                new JdbcPeriodicRuleDiscovererFactory(
                        JdbcConnectorOptions.builder()
                                .setTableName("public.cep_rules")
                                .setDriverName("org.postgresql.Driver")
                                .setDBUrl("jdbc:postgresql://20.5.2.35:5432/postgres")
                                .setUsername("postgres")
                                .setPassword("hollysys")
                                .build(),
                        3,
                        "udf",
                        Collections.emptyList(),
                        Duration.ofMinutes(1).toMillis()),
                TypeInformation.of(FieldRow.class),
                "cep-test",
                "/",
                true
        );

        alarms.print();

        env.execute("DynamicUdfExamples");
    }
}
