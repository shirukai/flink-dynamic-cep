package org.apache.flink.cep.utils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.jackson.JacksonMapperFactory;

/**
 * Jackson工具类
 *
 * @author shirukai
 */
public class JacksonUtils {
    private final static ObjectMapper DEFAULT_OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();

    public static ObjectMapper getObjectMapper() {
        return DEFAULT_OBJECT_MAPPER;
    }
}
