/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */
package com.alibaba.datax.plugin.reader.gdbreader.model;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * @author : Liu Jianping
 * @date : 2019/9/6
 */

@Data
public class GdbElement {
    String id = null;
    String label = null;
    String to = null;
    String from = null;
    String toLabel = null;
    String fromLabel = null;

    Map<String, Object> properties = new HashMap<>();

    public GdbElement() {
    }

    public GdbElement(String id, String label) {
        this.id = id;
        this.label = label;
    }

}
