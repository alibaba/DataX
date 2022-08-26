package com.alibaba.datax.plugin.unstructuredstorage.reader.binaryFileUtil;

import java.util.Arrays;

/**
 * @Author: guxuan
 * @Date 2022-05-17 16:00
 */
public class ByteUtils {

    private int size;
    private int kDefaultBufferSize = 0;
    private byte[] buffer;

    public byte[] getBuffer() {
        return buffer;
    }

    public ByteUtils() {
        buffer = new byte[0];
        size = 0;
    }

    public long getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public ByteUtils append(byte[] buf) {

        if (buf == null){
            return this;
        }
        buffer = Arrays.copyOf(buffer, buffer.length + buf.length);
        System.arraycopy(buf, 0, buffer, size, buf.length);
        size += buf.length;
        return this;
    }

    public void clear()
    {
        buffer = new byte[kDefaultBufferSize];
        size = 0;
    }
}
