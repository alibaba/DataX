package com.alibaba.datax.plugin.reader.txtfilereader.util;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import org.apache.commons.codec.binary.Base64;


/**
 * Base64 转换工具
 */
public class Base64Utils {


    /**
     * 加密函数，带压缩
     * @param encode  传入需要加密的字符串
     * @return
     */
    public static String encrypt( String encode){

//        // 3.将反转后的字符串，拼接上16位随机字符串在前面

//        String encode = encode(bytes);
        encode=ZipWithZlib.compressData(encode);
        encode=getRandomString(16)+encode;

        return encode;
    }



    /**
     * 解密函数
     * @param encode 传入加密后的字符串
     * @return
     */
    public static String decrypt(String encode){
//        // 1.去掉前面16位随机字符串
        encode=encode.substring(16);
        encode = ZipWithZlib.decompressData(encode);

        return encode;



    }




    public static String encryptByBytes( byte[] bytes){

//        // 3.将反转后的字符串，拼接上16位随机字符串在前面
        String encode=encode(bytes);
//        String encode = encode(bytes);
        encode=ZipWithZlib.compressData(encode);
        encode=getRandomString(16)+encode;

        return encode;
    }





    public static byte[] decryptByBytes(String encode){
//        // 1.去掉前面16位随机字符串
        encode=encode.substring(16);
        encode = ZipWithZlib.decompressData(encode);

        return decode(encode);



    }











    /**
     * Base64字符串 转换为 byte数组
     */
    public static byte[] decode(String base64) {

        return new Base64().decode(base64);
    }

    /**
     * byte数组 转换为 Base64字符串
     */
    public static String encode(byte[] data) {
        String encode = new Base64().encodeToString(data);
        encode=encode.replaceAll("\015","");
        return encode;
    }


    /**
     * 将字符串反转
     * @param s
     * @return
     */
    public static String reverse(String s) {
        return new StringBuffer(s).reverse().toString();
    }

    /**
     * 生成随机字符串
     * @param length
     * @return
     */
    public static String getRandomString(int length){
        String str="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random=new Random();
        StringBuffer sb=new StringBuffer();
        for(int i=0;i<length;i++){
            int number=random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }






    /**
     * 把文件内容编码为 Base64字符串, 只能编码小文件（例如文本、图片等）
     */
    public static String encodeFile(File file) throws Exception {
        InputStream in = null;
        ByteArrayOutputStream bytesOut = null;

        try {
            in = new FileInputStream(file);
            bytesOut = new ByteArrayOutputStream((int) file.length());

            byte[] buf = new byte[1024];
            int len = -1;

            while ((len = in.read(buf)) != -1) {
                bytesOut.write(buf, 0, len);
            }
            bytesOut.flush();

            return encode(bytesOut.toByteArray());

        } finally {
            close(in);
            close(bytesOut);
        }
    }

    /**
     * 将字节数组写入文件
     * @param file
     * @param bytes
     */
    public static void writeImage(String file,byte[] bytes){
        try{
            FileOutputStream outStream = new FileOutputStream(file);
            //写入数据
            outStream.write(bytes);
            outStream.close();
        }catch(Exception e){
            e.printStackTrace();
        }

    }

    /**
     * 把 Base64字符串 转换为 byte数组, 保存到指定文件
     */
    public static void decodeFile(String base64, File file) throws Exception {
        OutputStream fileOut = null;
        try {
            fileOut = new FileOutputStream(file);
            fileOut.write(decode(base64));
            fileOut.flush();
        } finally {
            close(fileOut);
        }
    }

    private static void close(Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException e) {
                // nothing
            }
        }
    }




}