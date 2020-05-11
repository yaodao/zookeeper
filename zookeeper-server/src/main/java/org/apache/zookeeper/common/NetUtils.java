/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.common;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * This class contains common utilities for netstuff. Like printing IPv6 literals correctly
 */
public class NetUtils {

    // 将入参addr对象转成 "ip:port" 形式的字符串返回。
    public static String formatInetAddr(InetSocketAddress addr) {
        InetAddress ia = addr.getAddress();

        // 格式化串，返回串类似 "www.baidu.com:8081"
        if (ia == null) {
            return String.format("%s:%s", addr.getHostString(), addr.getPort());
        }

        if (ia instanceof Inet6Address) {
            return String.format("[%s]:%s", ia.getHostAddress(), addr.getPort());
        } else {
            // 返回串类似 "39.156.66.14:8081"
            return String.format("%s:%s", ia.getHostAddress(), addr.getPort());
        }
    }
}
