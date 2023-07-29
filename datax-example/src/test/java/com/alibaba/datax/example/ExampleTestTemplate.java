package com.alibaba.datax.example;

import org.junit.Before;

import java.io.File;

/**
 * {@code Author} FuYouJ
 * {@code Date} 2023/7/29 18:23
 */

public abstract class ExampleTestTemplate {

    @Before
    public void fixWorkingDirectory(){
        String property = System.getProperty("user.dir");
        File file = new File(property);
        File parentFile = file.getParentFile();
        System.setProperty("user.dir",parentFile.getAbsolutePath());
    }
}
