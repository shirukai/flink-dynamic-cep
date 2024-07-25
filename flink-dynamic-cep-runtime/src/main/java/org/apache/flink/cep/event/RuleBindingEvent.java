package org.apache.flink.cep.event;

import lombok.Data;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.List;

/**
 * 规则绑定事件
 *
 * @author shirukai
 */
@Data
public class RuleBindingEvent implements OperatorEvent {
    private static final long serialVersionUID = 4324510678890794952L;
    private List<RuleBinding> bindings;

    public RuleBindingEvent(List<RuleBinding> bindings) {
        this.bindings = bindings;
    }
}
