package org.apache.flink.cep.event;

import lombok.Data;

import java.io.Serializable;
import java.util.Set;

/**
 * 规则更新实体
 *
 * @author shirukai
 */
@Data
public class RuleUpdated implements Serializable {
    private String id;
    private Integer version;
    private String parameters;
    private String function;
    private String pattern;
    private Set<String> libs;

    public static RuleUpdated of(Rule rule) {
        RuleUpdated updated = new RuleUpdated();
        updated.id = rule.getId();
        updated.version = rule.getVersion();
        updated.parameters = rule.getParameters();
        updated.function = rule.getFunction();
        updated.pattern = rule.getPattern();
        updated.libs = rule.getLibs();
        return updated;
    }
}
