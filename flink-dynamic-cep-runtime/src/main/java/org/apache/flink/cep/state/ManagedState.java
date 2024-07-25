package org.apache.flink.cep.state;

import lombok.Setter;

import java.io.Serializable;

/**
 * 管理状态，用来包装用户自定义的状态对象
 *
 * @author shirukai
 */
@Setter
public class ManagedState implements Serializable {

    private static final long serialVersionUID = 8255913995370081345L;
    private Object userState;

    @SuppressWarnings("unchecked")
    public <S> S getUserState() {
        return (S) userState;
    }

}
