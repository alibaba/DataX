/**
 * (C) 2010-2014 Alibaba Group Holding Limited.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.datax.plugin.reader.tablestorereader;

public final class Key {
    /* ots account configuration */
    public final static String OTS_ENDPOINT = "endpoint";

    public final static String OTS_ACCESSID = "accessId";

    public final static String OTS_ACCESSKEY = "accessKey";

    public final static String OTS_INSTANCE_NAME = "instanceName";

    /**
     * 指定的表名
     */
    public final static String TABLE_NAME = "table";

    /**
     * 指定的索引名
     */
    public final static String INDEX_NAME = "index";

    public final static String COLUMN = "column";

    /**
     * 要导出的属性列name 为List结构 不能为空
     */
    public final static String COLUMN_NAME = "columnName";

    /**
     * 每次读取的大小，默认为1000
     */
    public final static String LIMIT = "limit";

    //======================================================
    // 注意：如果range-begin大于range-end,那么系统将逆序导出所有数据
    //======================================================
    // Range的组织格式
    // "range":{
    //   "begin":[],
    //   "end":[],
    //   "split":[]
    // }
    public final static String RANGE = "range";

    public final static String RANGE_BEGIN = "begin";

    public final static String RANGE_END = "end";

    public final static String RANGE_SPLIT = "split";

    public final static String QUERY = "query";
}
