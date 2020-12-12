package com.alibaba.datax.common.element;

/**
 * Created by jingxing on 14-8-24.
 */

public interface Record {
    
    void addColumn(Column column);
    
    void setColumn(int i, final Column column);
    
    Column getColumn(int i);
    
    String toString();
    
    int getColumnNumber();
    
    int getByteSize();
    
    int getMemorySize();
    
}
