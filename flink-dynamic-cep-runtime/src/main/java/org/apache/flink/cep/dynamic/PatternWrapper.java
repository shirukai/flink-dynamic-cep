package org.apache.flink.cep.dynamic;

import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier;

/**
 * 包装Pattern类，暴露构造方法
 * @author shirukai
 */
public class PatternWrapper<T, F extends T> extends Pattern<T,F> {
    public PatternWrapper(String name,
                          Pattern<T, ? extends T> previous,
                          Quantifier.ConsumingStrategy consumingStrategy,
                          AfterMatchSkipStrategy afterMatchSkipStrategy) {
        super(name, previous, consumingStrategy, afterMatchSkipStrategy);
    }
}
