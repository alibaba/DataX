package com.alibaba.datax.plugin.reader.obhbasereader.enums;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.reader.obhbasereader.HbaseReaderErrorCode;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

public enum FetchVersion {

    OLDEST("oldest"), LATEST("latest");

    private final String version;

    FetchVersion(String version) {
        this.version = version;
    }

    public static FetchVersion getByDesc(String name) {
        Optional<FetchVersion> result = Stream.of(values()).filter(v -> v.version.equalsIgnoreCase(name))
                .findFirst();
        return result.orElseThrow(() -> {
            return DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE,
                    String.format("obHBasereader 不支持该类型:%s, 目前支持的类型是:%s", name, Arrays.asList(values())));
        });
    }
}
