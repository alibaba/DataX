package com.alibaba.datax.plugin.reader.iotdbreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum IoTDBReaderErrorCode implements ErrorCode {

  CONF_ERROR("IoTDBReader-0", "Error Configuration");

  private final String code;
  private final String description;

  IoTDBReaderErrorCode(String code, String description) {
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
