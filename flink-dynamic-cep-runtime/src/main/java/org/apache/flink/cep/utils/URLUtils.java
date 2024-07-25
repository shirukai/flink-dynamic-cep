package org.apache.flink.cep.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * 根据baseUri和path转为URL
 * @author shirukai
 */
public class URLUtils {
    private static final Logger LOG = LoggerFactory.getLogger(URLUtils.class);

    public static URL toURL(String baseUri, String path) {
        try {
            if (baseUri.startsWith("http://")) {
                URL url = URI.create(baseUri).resolve(path).toURL();
                // 读取url中的数据写到临时的jar文件中
                try (InputStream inputStream = url.openStream()) {
                    Path tempPath = Files.createTempFile("flink-userlib-", ".jar");
                    LOG.info("Downloading {} to {}", url, tempPath);
                    Files.copy(inputStream, tempPath, StandardCopyOption.REPLACE_EXISTING);
                    return tempPath.toUri().toURL();
                }
            } else {
                return Paths.get(baseUri).resolve(path).toUri().toURL();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
