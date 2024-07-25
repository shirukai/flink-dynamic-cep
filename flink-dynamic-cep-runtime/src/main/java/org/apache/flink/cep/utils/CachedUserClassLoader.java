package org.apache.flink.cep.utils;

import lombok.Getter;
import org.apache.flink.util.FlinkUserCodeClassLoader;
import org.apache.flink.util.FlinkUserCodeClassLoaders;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Set;

/**
 * 用户自定义类加载器
 *
 * @author shirukai
 */
@Getter
public class CachedUserClassLoader {
    private final int version;
    private final URLClassLoader classLoader;

    private CachedUserClassLoader(int version, URLClassLoader classLoader) {

        this.version = version;
        this.classLoader = classLoader;
    }

    public static CachedUserClassLoader of(String libDir, Set<String> libs, int version, ClassLoader parent) {
        URL[] urls = libs.stream().map(lib -> URLUtils.toURL(libDir, lib)).toArray(URL[]::new);
        // 创建URL classloader
        URLClassLoader classLoader = FlinkUserCodeClassLoaders.create(FlinkUserCodeClassLoaders.ResolveOrder.CHILD_FIRST,
                urls,
                parent,
                new String[]{},
                FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER,
                false
        );
        return new CachedUserClassLoader(version, classLoader);
    }


}
