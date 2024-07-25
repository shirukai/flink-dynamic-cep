package org.apache.flink.cep.operator;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.event.EventRecord;
import org.apache.flink.cep.event.RuleBinding;
import org.apache.flink.cep.event.RuleBindingEvent;
import org.apache.flink.cep.utils.WildcardMatchUtils;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;

/**
 * 规则分发算子
 *
 * @author shirukai
 */
public class RuleDistributorOperator<IN> extends AbstractStreamOperator<EventRecord<IN>>
        implements OneInputStreamOperator<IN, EventRecord<IN>>,
        OperatorEventHandler {
    private transient TimestampedCollector<EventRecord<IN>> collector;

    /**
     * 全量的与节点绑定的规则列表
     */
    private List<RuleBinding> bindings;

    /**
     * Key绑定的规则映射，一个Key可能绑定多个规则
     */
    private KeyBindingMap keyBindings;

    /**
     * 规则分发器，Key规则绑定分发器、全量规则分发器
     */
    private transient RuleDistributor<IN> distributor;

    /**
     * 是否启用Key绑定，如果启用，只处理与Key绑定的规则
     */
    private final boolean keyBindingEnabled;

    public RuleDistributorOperator(ProcessingTimeService processingTimeService, boolean keyBindingEnabled) {
        this.processingTimeService = processingTimeService;
        this.keyBindingEnabled = keyBindingEnabled;
    }


    @Override
    public void open() throws Exception {
        super.open();
        collector = new TimestampedCollector<>(output);
        bindings = Collections.emptyList();
        keyBindings = new KeyBindingMap();
        if (keyBindingEnabled) {
            distributor = element -> {
                Object key = getCurrentKey();
                // 只分发Key绑定的规则
                Set<Tuple2<String, Integer>> keyedBindings = keyBindings.get(key.toString());
                if (keyedBindings != null) {
                    for (Tuple2<String, Integer> keyedBinding : keyedBindings) {
                        sendRecord(element, keyedBinding.f0, keyedBinding.f1);
                    }
                }

            };
        } else {
            distributor = element -> {
                // 分发所有的规则
                for (RuleBinding binding : bindings) {
                    sendRecord(element, binding.getId(), binding.getVersion());
                }
            };
        }
    }

    public void sendRecord(StreamRecord<IN> element, String ruleId, Integer version) {
        EventRecord<IN> record = new EventRecord<>();
        record.setEvent(element.getValue());
        record.setRuleId(ruleId);
        record.setRuleVersion(version);
        collector.collect(record);
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        RuleBindingEvent bindingEvent = ((RuleBindingEvent) evt);
        bindings = bindingEvent.getBindings();
        // 清空之前的绑定映射
        keyBindings.clear();
        LOG.info("Update rule bindings: {}", bindings);
        for (RuleBinding binding : bindings) {
            Tuple2<String, Integer> ruleVersion = Tuple2.of(binding.getId(), binding.getVersion());
            for (String bindingKey : binding.getBindingKeys()) {
                keyBindings.computeIfAbsent(bindingKey, s -> new HashSet<>()).add(ruleVersion);
            }
        }
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        collector.setTimestamp(element);
        distributor.distribute(element);
    }

    public interface RuleDistributor<IN> {
        void distribute(StreamRecord<IN> element);
    }

    public static class KeyBindingMap implements Serializable {
        /**
         * Key与规则的映射关系
         */
        private final Map<String, Set<Tuple2<String, Integer>>> keyBindingsMapping;

        /**
         * 通配符与绑定规则的映射关系
         */
        private final Map<String, Set<Tuple2<String, Integer>>> wildcardBindingsMapping;

        public KeyBindingMap() {
            keyBindingsMapping = new HashMap<>();
            wildcardBindingsMapping = new HashMap<>();
        }

        public Set<Tuple2<String, Integer>> get(String key) {
            Set<Tuple2<String, Integer>> bindings = keyBindingsMapping.getOrDefault(key,
                    wildcardBindingsMapping.getOrDefault(key,new HashSet<>()));
            if(bindings.isEmpty()){
                // 按照通配符查找
                for (Map.Entry<String, Set<Tuple2<String, Integer>>> bindingSet : wildcardBindingsMapping.entrySet()) {
                    if (WildcardMatchUtils.match(key, bindingSet.getKey())) {
                        bindings.addAll(bindingSet.getValue());
                    }
                }
            }
            return bindings;
        }

        public Set<Tuple2<String, Integer>> computeIfAbsent(String bindingWildcard,
                                                            Function<String, Set<Tuple2<String, Integer>>> mappingFunction) {

            if(WildcardMatchUtils.isWildcard(bindingWildcard)){
                return wildcardBindingsMapping.computeIfAbsent(bindingWildcard, mappingFunction);
            }else{
                return keyBindingsMapping.computeIfAbsent(bindingWildcard, mappingFunction);
            }

        }

        public void clear() {
            keyBindingsMapping.clear();
            wildcardBindingsMapping.clear();
        }
    }
}
