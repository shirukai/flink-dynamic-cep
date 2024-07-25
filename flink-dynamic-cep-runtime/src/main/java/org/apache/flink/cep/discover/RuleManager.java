package org.apache.flink.cep.discover;



import org.apache.flink.cep.event.Rule;

import java.util.List;

public interface RuleManager{
    void onRuleUpdated(List<Rule> rules);
}
