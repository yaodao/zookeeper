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

package org.apache.zookeeper.server.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;


public class ConfigUtils {
    // 解析入参configData串，返回"clienthost1:port,clienthost2:port"串，即，返回的是所有客户端的ip和端口
    static public String getClientConfigStr(String configData) {
        Properties props = new Properties();        
        try {
          // 从字符串中获取配置
          props.load(new StringReader(configData));
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
        StringBuffer sb = new StringBuffer();
        boolean first = true;
        String version = "";
        for (Entry<Object, Object> entry : props.entrySet()) {
             String key = entry.getKey().toString().trim();
             String value = entry.getValue().toString().trim();
             if (key.equals("version")) version = value;
             if (!key.startsWith("server.")) continue;

             // 生成QuorumServer对象，并通过解析value串给它的成员变量赋值
             QuorumPeer.QuorumServer qs;
             try {
               qs = new QuorumPeer.QuorumServer(-1, value);
             } catch (ConfigException e) {              
                    e.printStackTrace();
                    continue;
             }

            // 形成串 "clienthost:port"
             if (!first) sb.append(",");
             else first = false;
             if (null != qs.clientAddr) {
                 sb.append(qs.clientAddr.getHostString()
                         + ":" + qs.clientAddr.getPort());
             }
        }
        return version + " " + sb.toString();
    }

    /**
     * Gets host and port by spliting server config with support for IPv6 literals
     * @return String[] first element being the IP address and the next being the port
     * @param s server config, server:port
     */
    /**
     * 从入参s中获取主机名和端口，以数组形式返回。支持ipv6格式的串
     * 返回值数组的 第一个元素是主机名，剩下的元素为端口。
     *
     * 入参s举例： "127.0.0.1:1234:1236:participant"  或者  "[::1]:1234:1236:participant"
     */
    public static String[] getHostAndPort(String s)
            throws ConfigException
    {
        // 若s是带中括号那种简写的ipv6地址，则去掉中括号，剩余元素组成数组返回。
        if (s.startsWith("[")) {
            int i = s.indexOf("]:");
            if (i < 0) {
                throw new ConfigException(s + " starts with '[' but has no matching ']:'");
            }

            String[] sa = s.substring(i + 2).split(":");
            String[] nsa = new String[sa.length + 1];
            nsa[0] = s.substring(1, i);
            System.arraycopy(sa, 0, nsa, 1, sa.length);

            return nsa;
        }
        // s是普通ipv6 或者 ipv4，则直接分割返回数组。
        else {
            return s.split(":");
        }
    }
}
