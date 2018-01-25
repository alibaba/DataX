package com.alibaba.datax.plugin.unstructuredstorage.reader;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipCycleInputStream extends InputStream {
    private static final Logger LOG = LoggerFactory
            .getLogger(ZipCycleInputStream.class);

    private ZipInputStream zipInputStream;
    private ZipEntry currentZipEntry;

    public ZipCycleInputStream(InputStream in) {
        this.zipInputStream = new ZipInputStream(in);
    }

    @Override
    public int read() throws IOException {
        // 定位一个Entry数据流的开头
        if (null == this.currentZipEntry) {
            this.currentZipEntry = this.zipInputStream.getNextEntry();
            if (null == this.currentZipEntry) {
                return -1;
            } else {
                LOG.info(String.format("Validate zipEntry with name: %s",
                        this.currentZipEntry.getName()));
            }
        }

        // 不支持zip下的嵌套, 对于目录跳过
        if (this.currentZipEntry.isDirectory()) {
            LOG.warn(String.format("meet a directory %s, ignore...",
                    this.currentZipEntry.getName()));
            this.currentZipEntry = null;
            return this.read();
        }

        // 读取一个Entry数据流
        int result = this.zipInputStream.read();

        // 当前Entry数据流结束了, 需要尝试下一个Entry
        if (-1 == result) {
            this.currentZipEntry = null;
            return this.read();
        } else {
            return result;
        }
    }

    @Override
    public void close() throws IOException {
        this.zipInputStream.close();
    }
}
