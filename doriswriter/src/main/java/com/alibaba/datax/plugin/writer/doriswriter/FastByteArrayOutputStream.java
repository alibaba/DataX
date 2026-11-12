package com.alibaba.datax.plugin.writer.doriswriter;

import java.util.Arrays;

/**
 * Lightweight growable byte buffer to reduce intermediate object creation.
 */
public class FastByteArrayOutputStream {
    private byte[] buffer;
    private int count;

    public FastByteArrayOutputStream(int initialCapacity) {
        this.buffer = new byte[Math.max(initialCapacity, 1024)];
    }

    public void write(byte[] b) {
        write(b, 0, b.length);
    }

    public void write(byte[] b, int off, int len) {
        if (len <= 0) {
            return;
        }
        ensureCapacity(count + len);
        System.arraycopy(b, off, buffer, count, len);
        count += len;
    }

    public int size() {
        return count;
    }

    public void reset() {
        count = 0;
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(buffer, count);
    }

    private void ensureCapacity(int minCapacity) {
        if (minCapacity <= buffer.length) {
            return;
        }
        int newCapacity = buffer.length;
        while (newCapacity < minCapacity) {
            newCapacity = newCapacity << 1;
        }
        buffer = Arrays.copyOf(buffer, newCapacity);
    }
}
