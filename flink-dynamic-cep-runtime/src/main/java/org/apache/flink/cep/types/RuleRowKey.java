package org.apache.flink.cep.types;

import lombok.Data;
import org.apache.flink.api.java.functions.KeySelector;

import java.io.Serializable;
import java.util.Objects;

/**
 * 规则数据Key
 *
 * @author shirukai
 */
@Data
public class RuleRowKey<KEY> implements Serializable {
    private final static RuleRowKey<?> EMPTY = new RuleRowKey<>(null, null);
    private String ruleKey;
    private KEY userKey;

    private RuleRowKey(String ruleKey, KEY userKey) {
        this.ruleKey = ruleKey;
        this.userKey = userKey;
    }

    public static <KEY> RuleRowKey<KEY> of(String ruleKey, KEY userKey) {
        return new RuleRowKey<>(ruleKey, userKey);
    }

    public static <KEY> RuleRowKey<KEY> of(String ruleKey) {
        return new RuleRowKey<>(ruleKey, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RuleRowKey<?> that = (RuleRowKey<?>) o;
        return Objects.equals(ruleKey, that.ruleKey) && Objects.equals(userKey, that.userKey);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(ruleKey);
        result = 31 * result + Objects.hashCode(userKey);
        return result;
    }

    public static <T> KeySelector<T, RuleRowKey<?>> nullRowKeySelector() {
        return value -> EMPTY;
    }

}
