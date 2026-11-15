/**
 * %流程框架%
 * %1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.common;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * @类名: ByteAndStreamUtils
 * @说明: 字节与流工具类 
 *
 * @author   leehom 
 * @Date	 2010-4-2 下午02:19:25
 * 修改记录：
 *
 * @see 	 
 */
public class ByteAndStreamUtils {
	
	/**
	 * @说明：流转换成字节
	 *
	 * @author hjli
	 * @param is
	 * @return
	 * 
	 * @异常： 
	 */
	public static byte[] StreamToBytes(InputStream is) {
		BufferedInputStream bis = null;
        try {
            is = new BufferedInputStream(is);
            byte[] bytes = new byte[is.available()];
            int len = bytes.length;
            int offset = 0;
            int read = 0;
            while (offset < len
                    && (read = is.read(bytes, offset, len - offset)) > 0) {
                offset += read;
            }
            return bytes;
        } catch (Exception e) {
            return null;
        } finally {
            try {
                is.close();
                is = null;
            } catch (IOException e) {
                return null;
            }

        }
	}
	// A block of stream to bytes
	public static byte[] StreamBlockToBytes(InputStream is, long offset, int size) {
		BufferedInputStream bis = null;
        try {
            is = new BufferedInputStream(is);                        
            // Skip to the position  where to start receiving bytes
            is.skip(offset);
            // Actual data size that would be get
            int datSize = is.available()< size ? is.available() : size;  
            byte[] bytes = new byte[datSize];
            // Offset of data bytes which to start storing bytes
            int dataOffset = 0;
            int read = 0;
            while (dataOffset < size
                    && (read = is.read(bytes, dataOffset, datSize - dataOffset)) > 0) {
            	dataOffset += read;
            }
            return bytes;
        } catch (Exception e) {
            return null;
        } 
	}
	
    /** 
     * 从字节数组获取对象
     * @Author Sean.guo
     * @EditTime 2007-8-13 上午11:46:34
     */
    public static Object objectFromBytes(byte[] objBytes) throws IOException, ClassNotFoundException {
        if (objBytes == null || objBytes.length == 0) {
            return null;
        }
        ByteArrayInputStream bi = new ByteArrayInputStream(objBytes);
        ObjectInputStream oi = new ObjectInputStream(bi);
        return oi.readObject();
    }

    /** 
     * 从对象获取一个字节数组
     * @Author Sean.guo
     * @EditTime 2007-8-13 上午11:46:56
     */
    public static byte[] objectToBytes(Serializable obj) throws IOException {
        if (obj == null) {
            return null;
        }
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        ObjectOutputStream oo = new ObjectOutputStream(bo);
        oo.writeObject(obj);
        return bo.toByteArray();
    }
    
    // 深克隆对象
    public static Object deepClone(Serializable obj) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        ObjectOutputStream oo = new ObjectOutputStream(bo);
        oo.writeObject(obj);
        //
        ByteArrayInputStream bi = new ByteArrayInputStream(bo.toByteArray());
        ObjectInputStream oi = new ObjectInputStream(bi);
        return oi.readObject();
    }
    
}
