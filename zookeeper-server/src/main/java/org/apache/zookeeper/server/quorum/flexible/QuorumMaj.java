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

package org.apache.zookeeper.server.quorum.flexible;

import java.util.HashMap;
import java.util.Set;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 * This class implements a validator for majority quorums. The implementation is
 * straightforward.
 * 
 */
public class QuorumMaj implements QuorumVerifier {
    // 此zk集群全部机器集合，key是myid，value是zk集群中的一个主机
    private Map<Long, QuorumServer> allMembers = new HashMap<Long, QuorumServer>();
    // 可参与投票的主机
    private HashMap<Long, QuorumServer> votingMembers = new HashMap<Long, QuorumServer>();
    // 角色是Observer的主机，不参与选举，也不参与过半写成功策略，在不影响写性能的情况下提升集群的读性能。
    private HashMap<Long, QuorumServer> observingMembers = new HashMap<Long, QuorumServer>();
    // 表示当前的验证器的版本
    private long version = 0;
    // 可投票机器的半数， 例如 5台 half=2；   3台，half=1
    private int half;

    public int hashCode() {
        assert false : "hashCode not designed";
        return 42; // any arbitrary constant will do, 任意常数都行
    }

    // 比较入参o和当前对象是否相等
    public boolean equals(Object o) {
        if (!(o instanceof QuorumMaj)) {
            return false;
        }
        QuorumMaj qm = (QuorumMaj) o;
        // 若当前对象的version值和入参o的version值相等， 则返回true
        if (qm.getVersion() == version)
            return true;
        // 若当前对象的allMembers的size不等于入参o的allMembers的size，则返回false
        if (allMembers.size() != qm.getAllMembers().size())
            return false;
        // 依次比较入参o和当前对象的allMembers中的每个元素，不等返回false
        for (QuorumServer qs : allMembers.values()) {
            QuorumServer qso = qm.getAllMembers().get(qs.id);
            if (qso == null || !qs.equals(qso))
                return false;
        }
        return true;
    }

    /**
     * Defines a majority to avoid computing it every time.
     * 
     */
    // 从入参allMembers中取元素，填充成员变量votingMembers和observingMembers
    public QuorumMaj(Map<Long, QuorumServer> allMembers) {
        this.allMembers = allMembers;
        for (QuorumServer qs : allMembers.values()) {
            if (qs.type == LearnerType.PARTICIPANT) {
                votingMembers.put(Long.valueOf(qs.id), qs);
            } else {
                observingMembers.put(Long.valueOf(qs.id), qs);
            }
        }
        half = votingMembers.size() / 2;
    }

    /**
     * QuorumMaj验证器的构造函数，
     * 从入参props中取配置项，只取"server."开头的配置项，填充成员变量allMembers，votingMembers和observingMembers
     *
     * @param props 解析配置文件得到的对象
     * @throws ConfigException
     */
    public QuorumMaj(Properties props) throws ConfigException {
        for (Entry<Object, Object> entry : props.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();

            if (key.startsWith("server.")) {
                int dot = key.indexOf('.');
                long sid = Long.parseLong(key.substring(dot + 1));
                QuorumServer qs = new QuorumServer(sid, value);
                allMembers.put(Long.valueOf(sid), qs);
                if (qs.type == LearnerType.PARTICIPANT)
                    votingMembers.put(Long.valueOf(sid), qs);
                else {
                    observingMembers.put(Long.valueOf(sid), qs);
                }
            } else if (key.equals("version")) {
                version = Long.parseLong(value, 16);
            }
        }
        half = votingMembers.size() / 2;
    }

    /**
     * Returns weight of 1 by default.
     * 
     * @param id
     */
    public long getWeight(long id) {
        return (long) 1;
    }

    // 将zk集群中每个机器的ip:port:type输出
    public String toString() {
        StringBuilder sw = new StringBuilder();

        // 对每个QuorumServer对象生成一个串 “server.1=192.168.1.11:2888:3888:participant\n”
        for (QuorumServer member : getAllMembers().values()) {
            String key = "server." + member.id;
            String value = member.toString();
            sw.append(key);
            sw.append('=');
            sw.append(value);
            sw.append('\n');
        }
        String hexVersion = Long.toHexString(version);
        sw.append("version=");
        sw.append(hexVersion);
        return sw.toString();
    }    

    /**
     * Verifies if a set is a majority. Assumes that ackSet contains acks only
     * from votingMembers
     */
    // 判断投票是否通过，只要发送ack的服务器数量大于一半就表示投票成功
    public boolean containsQuorum(Set<Long> ackSet) {
        return (ackSet.size() > half);
    }

    public Map<Long, QuorumServer> getAllMembers() {
        return allMembers;
    }

    public Map<Long, QuorumServer> getVotingMembers() {
        return votingMembers;
    }

    public Map<Long, QuorumServer> getObservingMembers() {
        return observingMembers;
    }

    public long getVersion() {
        return version;
    }
    
    public void setVersion(long ver) {
        version = ver;
    }
}
