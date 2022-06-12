package com.alibaba.datax.plugin.unstructuredstorage;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.Constant;
import com.alibaba.datax.plugin.unstructuredstorage.reader.Key;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderErrorCode;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

/**
 * @Author: guxuan
 * @Date 2022-05-17 16:04
 */
public enum  FileFormat {
    TEXT("text"),
    CSV("csv"),
    EXCEL("excel"),
    BINARY("binary");

    private String fileFormat;

    private boolean isText;
    private boolean isCsv;
    private boolean isExcel;
    private boolean isBinary;

    FileFormat(String fileFormat) {
        this.fileFormat = fileFormat.toLowerCase();
    }

    /**
     * 获取文件类型: 目前支持text,csv,excel,binary
     * @param configuration
     * @return
     */
    public static FileFormat getFileFormatByConfiguration(Configuration configuration) {
        String fileFormat = configuration.getString(Key.FILE_FORMAT, Constant.DEFAULT_FILE_FORMAT);
        return FileFormat.getByTypeName(fileFormat);
    }

    public String getFileFormat() {
        return this.fileFormat;
    }

    public static FileFormat getByTypeName(String fileFormat) {
        for (FileFormat fFormat : values()) {
            if (fFormat.fileFormat.equalsIgnoreCase(fileFormat)) {
                return fFormat;
            }
        }
        throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
               String.format("DataX 不支持该 fileFormat 类型:%s, 目前支持的 fileFormat 类型是:%s", fileFormat, Arrays.asList(values())));
    }

    public boolean equalsIgnoreCase(String fileFormat){
        return StringUtils.equalsIgnoreCase(fileFormat, this.fileFormat);
    }

    public boolean isText() {
        return this.equalsIgnoreCase(Constant.FILE_FORMAT_TEXT);
    }

    public void setText(boolean text) {
        isText = text;
    }

    public boolean isCsv() {
        return this.equalsIgnoreCase(Constant.FILE_FORMAT_CSV);
    }

    public void setCsv(boolean csv) {
        isCsv = csv;
    }

    public boolean isExcel() {
        return this.equalsIgnoreCase(Constant.FILE_FORMAT_EXCEL);
    }

    public void setExcel(boolean excel) {
        isExcel = excel;
    }

    public boolean isBinary() {
        return this.equalsIgnoreCase(Constant.FILE_FORMAT_BINARY);
    }

    public void setBinary(boolean binary) {
        isBinary = binary;
    }

    @Override
    public String toString(){
        return this.fileFormat;
    }
}
