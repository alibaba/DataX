package com.alibaba.datax.plugin.s3common.util;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.parquet.io.api.Binary;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * Author: duhanmin
 * Description:
 * Date: 2021/5/19 15:46
 */
public class HdfsUtil {

    public static final String NULL_VALUE = "\\N";
    private static final long NANO_SECONDS_PER_DAY = 86400_000_000_000L;

    private static final long JULIAN_EPOCH_OFFSET_DAYS = 2440588;

    private static final double SCALE_TWO = 2.0;
    private static final double SCALE_TEN = 10.0;
    private static final int BIT_SIZE = 8;

    public static Binary decimalToBinary(HiveDecimal hiveDecimal, int prec, int scale) {
        byte[] decimalBytes = hiveDecimal.bigIntegerBytesScaled(scale);
        // Estimated number of bytes needed.
        int precToBytes = ParquetHiveSerDe.PRECISION_TO_BYTE_COUNT[prec - 1];
        if (precToBytes == decimalBytes.length) {
            // No padding needed.
            return Binary.fromByteArray(decimalBytes);
        }

        byte[] tgt = new byte[precToBytes];
        if (hiveDecimal.signum() == -1) {
            // For negative number, initializing bits to 1
            for (int i = 0; i < precToBytes; i++) {
                tgt[i] |= 0xFF;
            }
        }

        System.arraycopy(decimalBytes, 0, tgt, precToBytes - decimalBytes.length, decimalBytes.length); // Padding leading zeroes/ones.
        return Binary.fromByteArray(tgt);
    }

    /**
     * hive Decimal转换
     * @param decimalInfo
     * @param rowData
     * @return
     */
    public static HiveDecimalWritable getDecimalWritable(String rowData, ColumnTypeUtil.DecimalInfo decimalInfo){
        HiveDecimal hiveDecimal = HiveDecimal.create(new BigDecimal(rowData));
        hiveDecimal = HiveDecimal.enforcePrecisionScale(hiveDecimal, decimalInfo.getPrecision(), decimalInfo.getScale());
        if(hiveDecimal == null){
            String msg = String.format("数据数据[%s]precision和scale和元数据不匹配:decimal(%s, %s)", decimalInfo.getPrecision(), decimalInfo.getScale(), rowData);
            throw new RuntimeException(msg, new IllegalArgumentException());
        }

        return new HiveDecimalWritable(hiveDecimal);
    }

    public static Buffer getBufferFromDecimal(HiveDecimal dec, int scale) {
        if (dec == null) return null;
        ByteBuffer bb = ByteBuffer.wrap(dec.bigIntegerBytesScaled(scale));
        return bb;
    }

    public static int computeMinBytesForPrecision(int precision){
        int numBytes = 1;
        while (Math.pow(SCALE_TWO, BIT_SIZE * numBytes - 1.0) < Math.pow(SCALE_TEN, precision)) {
            numBytes += 1;
        }
        return numBytes;
    }

    public static byte[] longToByteArray(long data){
        long nano = data * 1000_000;

        int julianDays = (int) ((nano / NANO_SECONDS_PER_DAY) + JULIAN_EPOCH_OFFSET_DAYS);
        byte[] julianDaysBytes = getBytes(julianDays);
        flip(julianDaysBytes);

        long lastDayNanos = nano % NANO_SECONDS_PER_DAY;
        byte[] lastDayNanosBytes = getBytes(lastDayNanos);
        flip(lastDayNanosBytes);

        byte[] dst = new byte[12];

        System.arraycopy(lastDayNanosBytes, 0, dst, 0, 8);
        System.arraycopy(julianDaysBytes, 0, dst, 8, 4);

        return dst;
    }

    private static byte[] getBytes(long i) {
        byte[] bytes=new byte[8];
        bytes[0]=(byte)((i >> 56) & 0xFF);
        bytes[1]=(byte)((i >> 48) & 0xFF);
        bytes[2]=(byte)((i >> 40) & 0xFF);
        bytes[3]=(byte)((i >> 32) & 0xFF);
        bytes[4]=(byte)((i >> 24) & 0xFF);
        bytes[5]=(byte)((i >> 16) & 0xFF);
        bytes[6]=(byte)((i >> 8) & 0xFF);
        bytes[7]=(byte)(i & 0xFF);
        return bytes;
    }

    /**
     * @param bytes
     */
    private static void flip(byte[] bytes) {
        for(int i=0,j=bytes.length-1;i<j;i++,j--) {
            byte t=bytes[i];
            bytes[i]=bytes[j];
            bytes[j]=t;
        }
    }

    public static ObjectInspector columnTypeToObjectInspetor(ColumnType columnType) {
        ObjectInspector objectInspector = null;
        switch(columnType) {
            case TINYINT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Byte.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case SMALLINT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Short.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case INT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case BIGINT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case FLOAT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Float.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case DOUBLE:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Double.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case DECIMAL:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(HiveDecimalWritable.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case TIMESTAMP:

                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(TimestampWritableV2.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case DATE:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(DateWritableV2.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case BOOLEAN:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Boolean.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case BINARY:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(BytesWritable.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            default:
                throw new IllegalArgumentException("You should not be here");
        }
        return objectInspector;
    }
}

