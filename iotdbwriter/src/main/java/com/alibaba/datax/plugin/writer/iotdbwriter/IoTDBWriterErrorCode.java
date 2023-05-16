package com.alibaba.datax.plugin.writer.iotdbwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum IoTDBWriterErrorCode implements ErrorCode  {

  CONF_ERROR("IoTDBWriter-0", "Error Configuration"),
  WRITE_ERROR("IoTDBWriter-1", "Error Writing Data");

  private final String code;
  private final String description;

  IoTDBWriterErrorCode(String code, String description) {
    this.code = code;
    this.description = description;
  }

  @Override
  public String getCode() {
    return code;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public String toString() {
    return "IoTDBWriterErrorCode{" +
      "code='" + code + '\'' +
      ", description='" + description + '\'' +
      '}';
  }
}
