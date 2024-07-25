package org.apache.flink.cep.event;

import lombok.Data;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

/**
 * 规则实体
 *
 * @author shirukai
 */
@Data
public class Rule implements Serializable {
    private String id;
    private Integer version;
    private String parameters;
    private String function;
    private String pattern;
    private Set<String> libs;
    private Set<String> bindingKeys;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Rule rule = (Rule) o;

        if (!Objects.equals(id, rule.id)) return false;
        if (!Objects.equals(version, rule.version)) return false;
        if (!Objects.equals(parameters, rule.parameters)) return false;
        if (!Objects.equals(function, rule.function)) return false;
        if (!Objects.equals(pattern, rule.pattern)) return false;
        if (!Objects.equals(libs, rule.libs)) return false;
        return Objects.equals(bindingKeys, rule.bindingKeys);
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (parameters != null ? parameters.hashCode() : 0);
        result = 31 * result + (function != null ? function.hashCode() : 0);
        result = 31 * result + (pattern != null ? pattern.hashCode() : 0);
        result = 31 * result + (libs != null ? libs.hashCode() : 0);
        result = 31 * result + (bindingKeys != null ? bindingKeys.hashCode() : 0);
        return result;
    }
}
