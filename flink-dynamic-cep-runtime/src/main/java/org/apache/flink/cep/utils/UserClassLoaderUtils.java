package org.apache.flink.cep.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 用户ClassLoader工具类
 *
 * @author shirukai
 */
public class UserClassLoaderUtils {
    private static final Map<Set<String>, CachedUserClassLoader> CACHED = new ConcurrentHashMap<>(16);
    private static final Logger LOG = LoggerFactory.getLogger(UserClassLoaderUtils.class);

    public static ClassLoader getClassLoader(String libDir, Set<String> libs, int version, ClassLoader parent) {
        return CACHED.compute(libs, (strings, cached) -> {
            if (cached == null || cached.getVersion() < version) {
                LOG.info("Create classloader from urls: {}", libs);
                cached = CachedUserClassLoader.of(libDir, libs, version, parent);
            }
            return cached;
        }).getClassLoader();
    }


}
