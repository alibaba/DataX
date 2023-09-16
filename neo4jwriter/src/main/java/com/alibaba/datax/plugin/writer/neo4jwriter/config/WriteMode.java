package com.alibaba.datax.plugin.writer.neo4jwriter.config;


import java.util.Arrays;
import java.util.Optional;

/**
 * @author fuyouj
 */

public enum WriteMode {
    INSERT,
    UPDATE;

    public static Optional<WriteMode> from(String name){
        return Arrays.stream(WriteMode.values())
                .filter(e -> e.name().equalsIgnoreCase(name))
                .findFirst();
    }
}
