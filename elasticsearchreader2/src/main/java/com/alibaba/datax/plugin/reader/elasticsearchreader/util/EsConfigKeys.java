/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.datax.plugin.reader.elasticsearchreader.util;

/**
 * Configuration Keys for EsDataWriter and EsDataReader
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class EsConfigKeys {

    public static final String KEY_ADDRESS = "address";

    public static final String KEY_USERNAME = "username";

    public static final String KEY_PASSWORD = "password";

    public static final String KEY_QUERY = "query";

    public static final String KEY_INDEX = "index";

    public static final String KEY_TYPE = "type";

    public static final String KEY_BATCH_SIZE = "batchSize";

    public static final String KEY_BULK_ACTION = "bulkAction";

    public static final String KEY_COLUMN_NAME = "name";

    public static final String KEY_COLUMN_TYPE = "type";

    public static final String KEY_ID_COLUMN = "idColumn";

    public static final String KEY_ID_COLUMN_INDEX = "index";

    public static final String KEY_ID_COLUMN_TYPE = "type";

    public static final String KEY_ID_COLUMN_VALUE = "value";

    public static final String KEY_TIMEOUT = "timeout";

    public static final String KEY_PATH_PREFIX = "pathPrefix";
    public static final String SPLIT_INDEX_KEY = "SPLIT_INDEX_KEY";
    public static final String SPLIT_ADVICE_NUMBER = "SPLIT_ADVICE_NUMBER";

}
