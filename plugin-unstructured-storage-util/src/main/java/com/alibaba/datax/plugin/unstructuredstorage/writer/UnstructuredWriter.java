package com.alibaba.datax.plugin.unstructuredstorage.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface UnstructuredWriter extends Closeable {

    public void writeOneRecord(List<String> splitedRows) throws IOException;

    public void flush() throws IOException;

    public void close() throws IOException;

}
