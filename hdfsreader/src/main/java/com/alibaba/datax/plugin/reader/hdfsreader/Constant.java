package com.alibaba.datax.plugin.reader.hdfsreader;

import java.util.Arrays;
import java.util.List;

/**
 * Created by mingya.wmy on 2015/8/14.
 */
public class Constant {

  public static final String SOURCE_FILES = "sourceFiles";
  public static final String TEXT = "TEXT";
  public static final String ORC = "ORC";
  public static final String CSV = "CSV";
  public static final String SEQ = "SEQ";
  public static final String RC = "RC";
  public static final List<String> supportFileTypes = Arrays.asList(ORC, TEXT, CSV, SEQ, RC);

  public static void main(String[] args) {
    System.out.println(supportFileTypes);
  }
}
