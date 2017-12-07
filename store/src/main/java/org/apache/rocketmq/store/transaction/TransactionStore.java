/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.store.transaction;

import java.util.List;


/**
 * 数据库存储接口类
 * @author root
 *
 */
public interface TransactionStore {

    boolean load();

    boolean createTableIfNotExists();

    void close();

    boolean parpare(final List<TransactionRecord> trs);

    boolean confirm(final List<Long> pks);

    List<TransactionRecord> traverse(final long pk, final int nums);

    long totalRecords();

    long minPK();

    long maxPK();

    void setTableSuffix(int tableSuffix);
}
