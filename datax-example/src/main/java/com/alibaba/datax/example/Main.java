package com.alibaba.datax.example;


import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.Engine;
import com.alibaba.datax.example.util.ExampleConfigParser;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

/**
 * @author fuyouj
 */
public class Main {
    public static void main(String[] args) throws URISyntaxException {
        URL resource = Main.class.getResource("/job/stream2stream.json");
        URI uri = resource.toURI();
        String path = Paths.get(uri).toString();
        Configuration configuration = ExampleConfigParser.parse(path);
        Engine engine = new Engine();
        engine.start(configuration);
    }
}
