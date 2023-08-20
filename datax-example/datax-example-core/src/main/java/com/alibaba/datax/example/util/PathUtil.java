package com.alibaba.datax.example.util;


import com.alibaba.datax.common.exception.DataXException;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

/**
 * @author fuyouj
 */
public class PathUtil {
    public static String getAbsolutePathFromClassPath(String path) {
        URL resource = PathUtil.class.getResource(path);
        try {
            assert resource != null;
            URI uri = resource.toURI();
            return Paths.get(uri).toString();
        } catch (NullPointerException | URISyntaxException e) {
            throw DataXException.asDataXException("path error,please check whether the path is correct");
        }

    }
}
