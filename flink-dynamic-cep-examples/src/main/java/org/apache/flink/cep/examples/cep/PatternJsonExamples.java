package org.apache.flink.cep.examples.cep;

import org.apache.flink.cep.dynamic.impl.json.util.CepJsonUtils;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

/**
 * @author shirukai
 */
public class PatternJsonExamples {
    public static void main(String[] args) throws Exception {
        Pattern<Row, Row> pattern = Pattern.<Row>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new MyCondition())
                .times(3)
                // 连续3次异常
                .consecutive();

        System.out.println(CepJsonUtils.convertPatternToJSONString(pattern));
    }
}
