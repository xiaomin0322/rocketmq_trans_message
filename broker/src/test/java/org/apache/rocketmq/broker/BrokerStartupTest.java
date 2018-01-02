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
package org.apache.rocketmq.broker;


/**
 * broker的vm参数
-server
-Xms1g
-Xmx1g
-Xmn256m
-XX:+UseG1GC
-XX:G1HeapRegionSize=16m
-XX:G1ReservePercent=25
-XX:InitiatingHeapOccupancyPercent=30
-XX:SoftRefLRUPolicyMSPerMB=0
-XX:SurvivorRatio=8
-XX:+DisableExplicitGC
-Djava.ext.dirs=/Users/fengjian/Documents/rock_home
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/Users/fengjian/store/
-Drocketmq.broker.diskSpaceCleanForciblyRatio=0.95
-Drocketmq.broker.diskSpaceWarningLevelRatio=0.99
 * @author root
 *
 */
public class BrokerStartupTest {
   public static void main(String[] args)throws Exception {
	   //-n 127.0.0.1:9876 -c  /xxxx/incubator-rocketmq/conf/broker.conf
	   
	   System.setProperty("ROCKETMQ_HOME", "G:\\software\\apache-rocketmq-all");

	   args = new String[] {"-n","127.0.0.1:9876","-c","E:\\eclipse-workspace\\rocketmq_trans_message\\conf\\broker.conf"};
	   BrokerStartup.main(args);
	   System.in.read();
}
}
