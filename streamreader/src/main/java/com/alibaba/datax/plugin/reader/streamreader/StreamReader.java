package com.alibaba.datax.plugin.reader.streamreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamReader extends Reader {

	public static class Job extends Reader.Job {

	    private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
	    private Pattern mixupFunctionPattern;
		private Configuration originalConfig;

		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();
			// warn: 忽略大小写
			this.mixupFunctionPattern = Pattern.compile(Constant.MIXUP_FUNCTION_PATTERN, Pattern.CASE_INSENSITIVE);
			dealColumn(this.originalConfig);

			Long sliceRecordCount = this.originalConfig
					.getLong(Key.SLICE_RECORD_COUNT);
			if (null == sliceRecordCount) {
				throw DataXException.asDataXException(StreamReaderErrorCode.REQUIRED_VALUE,
						"没有设置参数[sliceRecordCount].");
			} else if (sliceRecordCount < 1) {
				throw DataXException.asDataXException(StreamReaderErrorCode.ILLEGAL_VALUE,
						"参数[sliceRecordCount]不能小于1.");
			}

		}

		private void dealColumn(Configuration originalConfig) {
			List<JSONObject> columns = originalConfig.getList(Key.COLUMN,
					JSONObject.class);
			if (null == columns || columns.isEmpty()) {
				throw DataXException.asDataXException(StreamReaderErrorCode.REQUIRED_VALUE,
						"没有设置参数[column].");
			}

			List<String> dealedColumns = new ArrayList<String>();
			for (JSONObject eachColumn : columns) {
				Configuration eachColumnConfig = Configuration.from(eachColumn);
				try {
                    this.parseMixupFunctions(eachColumnConfig);
                } catch (Exception e) {
                    throw DataXException.asDataXException(StreamReaderErrorCode.NOT_SUPPORT_TYPE,
                            String.format("解析混淆函数失败[%s]", e.getMessage()), e);
                }
				
				String typeName = eachColumnConfig.getString(Constant.TYPE);
				if (StringUtils.isBlank(typeName)) {
					// empty typeName will be set to default type: string
					eachColumnConfig.set(Constant.TYPE, Type.STRING);
				} else {
					if (Type.DATE.name().equalsIgnoreCase(typeName)) {
						boolean notAssignDateFormat = StringUtils
								.isBlank(eachColumnConfig
										.getString(Constant.DATE_FORMAT_MARK));
						if (notAssignDateFormat) {
							eachColumnConfig.set(Constant.DATE_FORMAT_MARK,
									Constant.DEFAULT_DATE_FORMAT);
						}
					}
					if (!Type.isTypeIllegal(typeName)) {
						throw DataXException.asDataXException(
								StreamReaderErrorCode.NOT_SUPPORT_TYPE,
								String.format("不支持类型[%s]", typeName));
					}
				}
				dealedColumns.add(eachColumnConfig.toJSON());
			}

			originalConfig.set(Key.COLUMN, dealedColumns);
		}
		
		private void parseMixupFunctions(Configuration eachColumnConfig) throws Exception{
		    // 支持随机函数, demo如下:
            // LONG: random 0, 10 0到10之间的随机数字
            // STRING: random 0, 10 0到10长度之间的随机字符串
            // BOOL: random 0, 10 false 和 true出现的比率
            // DOUBLE: random 0, 10 0到10之间的随机浮点数
            // DATE: random 2014-07-07 00:00:00, 2016-07-07 00:00:00 开始时间->结束时间之间的随机时间，日期格式默认(不支持逗号)yyyy-MM-dd HH:mm:ss
            // BYTES: random 0, 10 0到10长度之间的随机字符串获取其UTF-8编码的二进制串
            // 配置了混淆函数后，可不配置value
            // 2者都没有配置
            String columnValue = eachColumnConfig.getString(Constant.VALUE);
            String columnMixup = eachColumnConfig.getString(Constant.RANDOM);
            if (StringUtils.isBlank(columnMixup)) {
                eachColumnConfig.getNecessaryValue(Constant.VALUE,
                        StreamReaderErrorCode.REQUIRED_VALUE);
            }
            // 2者都有配置
            if (StringUtils.isNotBlank(columnMixup) && StringUtils.isNotBlank(columnValue)) {
                LOG.warn(String.format("您配置了streamreader常量列(value:%s)和随机混淆列(random:%s), 常量列优先", columnValue, columnMixup));
                eachColumnConfig.remove(Constant.RANDOM);
            }
		    if (StringUtils.isNotBlank(columnMixup)) {
		        Matcher matcher= this.mixupFunctionPattern.matcher(columnMixup);
		        if (matcher.matches()) {
		            String param1 = matcher.group(1);
		            long param1Int = 0;
		            String param2 = matcher.group(2);
		            long param2Int = 0;
		            if (StringUtils.isBlank(param1) && StringUtils.isBlank(param2)) {
		                throw DataXException.asDataXException(
	                            StreamReaderErrorCode.ILLEGAL_VALUE,
	                            String.format("random混淆函数不合法[%s], 混淆函数random的参数不能为空:%s, %s", columnMixup, param1, param2));
		            }
		            String typeName = eachColumnConfig.getString(Constant.TYPE);
		            if (Type.DATE.name().equalsIgnoreCase(typeName)) {
		                String dateFormat = eachColumnConfig.getString(Constant.DATE_FORMAT_MARK, Constant.DEFAULT_DATE_FORMAT);
		                try{
    		                SimpleDateFormat format = new SimpleDateFormat(
                                    eachColumnConfig.getString(Constant.DATE_FORMAT_MARK, Constant.DEFAULT_DATE_FORMAT));
    		                //warn: do no concern int -> long
                            param1Int = format.parse(param1).getTime();//milliseconds
                            param2Int = format.parse(param2).getTime();//milliseconds
		                }catch (ParseException e) {
		                    throw DataXException.asDataXException(
	                                StreamReaderErrorCode.ILLEGAL_VALUE,
	                                String.format("dateFormat参数[%s]和混淆函数random的参数不匹配，解析错误:%s, %s", dateFormat, param1, param2), e);
		                }
		            } else {
	                    param1Int = Integer.parseInt(param1);
	                    param2Int = Integer.parseInt(param2);
		            }
		            if (param1Int < 0 || param2Int < 0) {
		                throw DataXException.asDataXException(
                                StreamReaderErrorCode.ILLEGAL_VALUE,
                                String.format("random混淆函数不合法[%s], 混淆函数random的参数不能为负数:%s, %s", columnMixup, param1, param2));
		            }
		            if (!Type.BOOL.name().equalsIgnoreCase(typeName)) {
		                if (param1Int > param2Int) {
		                    throw DataXException.asDataXException(
	                                StreamReaderErrorCode.ILLEGAL_VALUE,
	                                String.format("random混淆函数不合法[%s], 混淆函数random的参数需要第一个小于等于第二个:%s, %s", columnMixup, param1, param2));
		                }
		            }
		            eachColumnConfig.set(Constant.MIXUP_FUNCTION_PARAM1, param1Int);
                    eachColumnConfig.set(Constant.MIXUP_FUNCTION_PARAM2, param2Int);
		        } else {
		            throw DataXException.asDataXException(
                            StreamReaderErrorCode.ILLEGAL_VALUE,
                            String.format("random混淆函数不合法[%s], 需要为param1, param2形式", columnMixup));
		        }
		        this.originalConfig.set(Constant.HAVE_MIXUP_FUNCTION, true);
		    }
		}

		@Override
		public void prepare() {
		}

		@Override
		public List<Configuration> split(int adviceNumber) {
			List<Configuration> configurations = new ArrayList<Configuration>();

			for (int i = 0; i < adviceNumber; i++) {
				configurations.add(this.originalConfig.clone());
			}
			return configurations;
		}

		@Override
		public void post() {
		}

		@Override
		public void destroy() {
		}

	}

	public static class Task extends Reader.Task {

		private Configuration readerSliceConfig;

		private List<String> columns;

		private long sliceRecordCount;
		
		private boolean haveMixupFunction;
		

		@Override
		public void init() {
			this.readerSliceConfig = super.getPluginJobConf();
			this.columns = this.readerSliceConfig.getList(Key.COLUMN,
					String.class);

			this.sliceRecordCount = this.readerSliceConfig
					.getLong(Key.SLICE_RECORD_COUNT);
            this.haveMixupFunction = this.readerSliceConfig.getBool(
                    Constant.HAVE_MIXUP_FUNCTION, false);
		}

		@Override
		public void prepare() {
		}

		@Override
		public void startRead(RecordSender recordSender) {
			Record oneRecord = buildOneRecord(recordSender, this.columns);
			while (this.sliceRecordCount > 0) {
                if (this.haveMixupFunction) {
                    oneRecord = buildOneRecord(recordSender, this.columns);
                }
				recordSender.sendToWriter(oneRecord);
				this.sliceRecordCount--;
			}
		}

		@Override
		public void post() {
		}

		@Override
		public void destroy() {
		}
		
		private Column buildOneColumn(Configuration eachColumnConfig) throws Exception {
		    String columnValue = eachColumnConfig
                    .getString(Constant.VALUE);
            Type columnType = Type.valueOf(eachColumnConfig.getString(
                    Constant.TYPE).toUpperCase());
            String columnMixup = eachColumnConfig.getString(Constant.RANDOM);
            long param1Int = eachColumnConfig.getLong(Constant.MIXUP_FUNCTION_PARAM1, 0L);
            long param2Int = eachColumnConfig.getLong(Constant.MIXUP_FUNCTION_PARAM2, 1L);
            boolean isColumnMixup = StringUtils.isNotBlank(columnMixup);
            
            switch (columnType) {
            case STRING:
                if (isColumnMixup) {
                    return new StringColumn(RandomStringUtils.randomAlphanumeric((int)RandomUtils.nextLong(param1Int, param2Int + 1)));
                } else {
                    return new StringColumn(columnValue);
                }
            case LONG:
                if (isColumnMixup) {
                    return new LongColumn(RandomUtils.nextLong(param1Int, param2Int + 1));
                } else {
                    return new LongColumn(columnValue);
                }
            case DOUBLE:
                if (isColumnMixup) {
                    return new DoubleColumn(RandomUtils.nextDouble(param1Int, param2Int + 1));
                } else {
                    return new DoubleColumn(columnValue);
                }
            case DATE:
                SimpleDateFormat format = new SimpleDateFormat(
                        eachColumnConfig.getString(Constant.DATE_FORMAT_MARK, Constant.DEFAULT_DATE_FORMAT));
                if (isColumnMixup) {
                    return new DateColumn(new Date(RandomUtils.nextLong(param1Int, param2Int + 1)));
                } else {
                    return new DateColumn(format.parse(columnValue));
                }
            case BOOL:
                if (isColumnMixup) {
                    // warn: no concern -10 etc..., how about (0, 0)(0, 1)(1,2)
                    if (param1Int == param2Int) {
                        param1Int = 0;
                        param2Int = 1;
                    }
                    if (param1Int == 0) {
                        return new BoolColumn(true);
                    } else if (param2Int == 0) {
                        return new BoolColumn(false);
                    } else {
                        long randomInt = RandomUtils.nextLong(0, param1Int + param2Int + 1);
                        return new BoolColumn(randomInt <= param1Int ? false : true);
                    }
                } else {
                    return new BoolColumn("true".equalsIgnoreCase(columnValue) ? true : false);
                }
            case BYTES:
                if (isColumnMixup) {
                    return new BytesColumn(RandomStringUtils.randomAlphanumeric((int)RandomUtils.nextLong(param1Int, param2Int + 1)).getBytes());
                } else {
                    return new BytesColumn(columnValue.getBytes());
                }
            default:
                // in fact,never to be here
                throw new Exception(String.format("不支持类型[%s]",
                        columnType.name()));
            }
		}

		private Record buildOneRecord(RecordSender recordSender,
				List<String> columns) {
			if (null == recordSender) {
				throw new IllegalArgumentException(
						"参数[recordSender]不能为空.");
			}

			if (null == columns || columns.isEmpty()) {
				throw new IllegalArgumentException(
						"参数[column]不能为空.");
			}

			Record record = recordSender.createRecord();
			try {
				for (String eachColumn : columns) {
					Configuration eachColumnConfig = Configuration.from(eachColumn);
					record.addColumn(this.buildOneColumn(eachColumnConfig));
				}
			} catch (Exception e) {
				throw DataXException.asDataXException(StreamReaderErrorCode.ILLEGAL_VALUE,
						"构造一个record失败.", e);
			}
			return record;
		}
	}

	private enum Type {
		STRING, LONG, BOOL, DOUBLE, DATE, BYTES, ;

		private static boolean isTypeIllegal(String typeString) {
			try {
				Type.valueOf(typeString.toUpperCase());
			} catch (Exception e) {
				return false;
			}

			return true;
		}
	}

}
