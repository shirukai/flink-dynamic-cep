package org.apache.flink.cep.functions;

import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;

/**
 * CEP条件抽象类，继承该抽象类实现的条件，会通过构造方法将全局参数传入进来
 *
 * @author shirukai
 */
public abstract class AbstractCondition<T> extends IterativeCondition<T> {

    public  void open(Configuration configuration) throws Exception{}

}
