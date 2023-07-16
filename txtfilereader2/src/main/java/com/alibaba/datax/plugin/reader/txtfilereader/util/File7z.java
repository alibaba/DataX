package com.alibaba.datax.plugin.reader.txtfilereader.util;

import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZOutputFile;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
/**
 * date: 2019/07/27
 * writed by yangtingting
 */
public class File7z {
    /**
     * 7z文件压缩
     *
     * @param inputFile  待压缩文件夹/文件名
     * @param outputFile 生成的压缩包名字
     */

    public static void Compress7z(String inputFile, String outputFile) throws Exception {
        File input = new File(inputFile);
        if (!input.exists()) {
            throw new Exception(input.getPath() + "待压缩文件不存在");
        }
        SevenZOutputFile out = new SevenZOutputFile(new File(outputFile));

        compress(out, input, null);
        out.close();
    }

    /**
     * @param name 压缩文件名，可以写为null保持默认
     */
    //递归压缩
    public static void compress(SevenZOutputFile out, File input, String name) throws IOException {
        if (name == null) {
            name = input.getName();
        }
        SevenZArchiveEntry entry = null;
        //如果路径为目录（文件夹）
        if (input.isDirectory()) {
            //取出文件夹中的文件（或子文件夹）
            File[] flist = input.listFiles();

            if (flist.length == 0)//如果文件夹为空，则只需在目的地.7z文件中写入一个目录进入
            {
                entry = out.createArchiveEntry(input,name + "/");
                out.putArchiveEntry(entry);
            } else//如果文件夹不为空，则递归调用compress，文件夹中的每一个文件（或文件夹）进行压缩
            {
                for (int i = 0; i < flist.length; i++) {
                    compress(out, flist[i], name + "/" + flist[i].getName());
                }
            }
        } else//如果不是目录（文件夹），即为文件，则先写入目录进入点，之后将文件写入7z文件中
        {
            FileInputStream fos = new FileInputStream(input);
            BufferedInputStream bis = new BufferedInputStream(fos);
            entry = out.createArchiveEntry(input, name);
            out.putArchiveEntry(entry);
            int len = -1;
            //将源文件写入到7z文件中
            byte[] buf = new byte[1024];
            while ((len = bis.read(buf)) != -1) {
                out.write(buf, 0, len);
            }
            bis.close();
            fos.close();
            out.closeArchiveEntry();
        }
    }

    public static void main (String[]args){
        try {
            Compress7z("D:\\YTT", "D:\\TestbyYTT.7z");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}