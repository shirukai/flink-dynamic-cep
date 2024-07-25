package org.apache.flink.cep.configuration;

import org.apache.flink.configuration.Configuration;

import java.util.Map;

/**
 * 以Object类型存储的配置
 *
 * @author shirukai
 */
public class ObjectConfiguration extends Configuration {
    public Object getObject(String key) {
        return confData.get(key);
    }

    public void setObject(String key, Object value) {
        confData.put(key, value);
    }

    public Map<String, Object> getRawMap() {
        return confData;
    }

    public static ObjectConfiguration of(Map<String, Object> map) {
        ObjectConfiguration configuration = new ObjectConfiguration();
        map.forEach(configuration::setObject);
        return configuration;
    }
}