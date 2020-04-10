/*
 * (C) 2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public
 * License version 2 as published by the Free Software Foundation.
 */
package com.alibaba.datax.plugin.writer.gdbwriter.util;

/**
 * @author : Liu Jianping
 * @date : 2019/8/3
 */

public class GdbDuplicateIdException extends Exception {
    public GdbDuplicateIdException(Exception e) {
        super(e);
    }

    public GdbDuplicateIdException() {
        super();
    }
}
