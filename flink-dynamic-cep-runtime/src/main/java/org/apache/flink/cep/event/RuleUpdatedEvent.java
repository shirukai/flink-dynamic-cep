package org.apache.flink.cep.event;

import lombok.Data;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.List;

/**
 * 规则更新事件
 *
 * @author shirukai
 */
@Data
public class RuleUpdatedEvent implements OperatorEvent {
    private static final long serialVersionUID = -6709401659018412949L;

    /**
     * 更新规则列表
     */
    private List<RuleUpdated> updates;


    public RuleUpdatedEvent(List<RuleUpdated> updates) {
        this.updates = updates;
    }
}
