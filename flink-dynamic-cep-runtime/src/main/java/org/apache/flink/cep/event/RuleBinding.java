package org.apache.flink.cep.event;

import lombok.Data;

import java.io.Serializable;
import java.util.Set;

/**
 * 规则绑定实体
 *
 * @author shirukai
 */
@Data
public class RuleBinding implements Serializable {
    private String id;
    private Integer version;
    private Set<String> bindingKeys;

    public static RuleBinding of(Rule rule) {
        RuleBinding binding = new RuleBinding();
        binding.id = rule.getId();
        binding.version = rule.getVersion();
        binding.bindingKeys = rule.getBindingKeys();
        return binding;
    }
}
