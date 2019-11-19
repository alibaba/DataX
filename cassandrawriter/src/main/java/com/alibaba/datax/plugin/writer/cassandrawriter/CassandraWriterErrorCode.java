package com.alibaba.datax.plugin.writer.cassandrawriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by mazhenlin on 2019/8/19.
 */
public enum CassandraWriterErrorCode implements ErrorCode {
  CONF_ERROR("CassandraWriter-00", "配置错误."),
  WRITE_DATA_ERROR("CassandraWriter-01", "写入数据时失败."),
  ;

  private final String code;
  private final String description;

  private CassandraWriterErrorCode(String code, String description) {
    this.code = code;
    this.description = description;
  }

  @Override
  public String getCode() {
    return this.code;
  }

  @Override
  public String getDescription() {
    return this.description;
  }

  @Override
  public String toString() {
    return String.format("Code:[%s], Description:[%s].", this.code, this.description);
  }
}
