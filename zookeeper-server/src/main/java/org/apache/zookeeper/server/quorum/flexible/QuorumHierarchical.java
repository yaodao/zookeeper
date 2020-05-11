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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;


/**
 * This class implements a validator for hierarchical quorums. With this
 * construction, zookeeper servers are split into disjoint groups, and 
 * each server has a weight. We obtain a quorum if we get more than half
 * of the total weight of a group for a majority of groups.
 * 
 * The configuration of quorums uses two parameters: group and weight. 
 * Groups are sets of ZooKeeper servers, and we set a group by passing
 * a colon-separated list of server ids. It is also necessary to assign
 * weights to server. Here is an example of a configuration that creates
 * three groups and assigns a weight of 1 to each server:
 * 
 *  group.1=1:2:3
 *  group.2=4:5:6
 *  group.3=7:8:9
 *  
 *  weight.1=1
 *  weight.2=1
 *  weight.3=1
 *  weight.4=1
 *  weight.5=1
 *  weight.6=1
 *  weight.7=1
 *  weight.8=1
 *  weight.9=1
 * 
 * Note that it is still necessary to define peers using the server keyword.
 */

public class QuorumHierarchical implements QuorumVerifier {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumHierarchical.class);

    // key是sid（serverid就是myid），value是该sid所占的权重
    private HashMap<Long, Long> serverWeight = new HashMap<Long, Long>();
    // key是sid，value是该sid所在的组id
    private HashMap<Long, Long> serverGroup = new HashMap<Long, Long>();
    // key是组id，value是该组的权重
    private HashMap<Long, Long> groupWeight = new HashMap<Long, Long>();

    // 组的数量
    private int numGroups = 0;
   
    private Map<Long, QuorumServer> allMembers = new HashMap<Long, QuorumServer>();
    private Map<Long, QuorumServer> participatingMembers = new HashMap<Long, QuorumServer>();
    private Map<Long, QuorumServer> observingMembers = new HashMap<Long, QuorumServer>();
    
    private long version = 0;
    
    public int hashCode() {
         assert false : "hashCode not designed";
         return 42; // any arbitrary constant will do ，任意常数都行
    }

    // 根据对象的成员变量，判断两个对象是否相同。
   public boolean equals(Object o){
       if (!(o instanceof QuorumHierarchical)) {
           return false;           
       }       
       QuorumHierarchical qm = (QuorumHierarchical)o;
       if (qm.getVersion() == version) return true;
       // 比较当前对象和入参o的成员变量的size
       if ((allMembers.size()!=qm.getAllMembers().size()) ||
           (serverWeight.size() != qm.serverWeight.size()) ||
           (groupWeight.size() != qm.groupWeight.size()) ||
            (serverGroup.size() != qm.serverGroup.size())) {
           return false;
       }
       // 对allMembers中的每个元素进行比较
       for (QuorumServer qs: allMembers.values()){
           QuorumServer qso = qm.getAllMembers().get(qs.id);
           if (qso == null || !qs.equals(qso)) return false;
       }
       // 对serverWeight中的每个元素进行比较
       for (Entry<Long, Long> entry : serverWeight.entrySet()) {
           if (!entry.getValue().equals(qm.serverWeight.get(entry.getKey())))
               return false;
       }
       // 对groupWeight中的每个元素进行比较
       for (Entry<Long, Long> entry : groupWeight.entrySet()) {
           if (!entry.getValue().equals(qm.groupWeight.get(entry.getKey())))
               return false;
       }
       // 对serverGroup中的每个元素进行比较
       for (Entry<Long, Long> entry : serverGroup.entrySet()) {
           if (!entry.getValue().equals(qm.serverGroup.get(entry.getKey())))
               return false;
       }
       return true;
   }
    /**
     * This contructor requires the quorum configuration
     * to be declared in a separate file, and it takes the
     * file as an input parameter.
     */
    public QuorumHierarchical(String filename)
    throws ConfigException {
        readConfigFile(filename);    
    }
    
    /**
     * This constructor takes a set of properties. We use
     * it in the unit test for this feature.
     */

    // 从入参qp中解析出集群中每台机器的权重，所属组，组的权重
    public QuorumHierarchical(Properties qp) throws ConfigException {
        parse(qp);
        LOG.info(serverWeight.size() + ", " + serverGroup.size() + ", " + groupWeight.size());
    }
  
    /**
     * Returns the weight of a server.
     * 
     * @param id
     */
    public long getWeight(long id){
        return serverWeight.get(id);
    }
    
    /**
     * Reads a configration file. Called from the constructor
     * that takes a file as an input.
     */
    // 从入参传入的文件，读取配置，并解析配置
    private void readConfigFile(String filename)
    throws ConfigException{
        File configFile = new File(filename);

        LOG.info("Reading configuration from: " + configFile);

        try {
            if (!configFile.exists()) {
                throw new IllegalArgumentException(configFile.toString()
                        + " file is missing");
            }
    
            Properties cfg = new Properties();
            FileInputStream in = new FileInputStream(configFile);
            try {
                cfg.load(in);
            } finally {
                in.close();
            }
    
            parse(cfg);
        } catch (IOException e) {
            throw new ConfigException("Error processing " + filename, e);
        } catch (IllegalArgumentException e) {
            throw new ConfigException("Error processing " + filename, e);
        }
        
    }
    
    
    /**
     * Parse properties if configuration given in a separate file.
     * Assumes that allMembers has been already assigned
     * @throws ConfigException 
     */
    // 从入参quorumProp中取配置，给当前对象的成员变量赋值
    private void parse(Properties quorumProp) throws ConfigException{
        for (Entry<Object, Object> entry : quorumProp.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString(); 

            // 若配置项以“server.”开头，则解析配置项，填充成员变量allMembers，participatingMembers，observingMembers
            if (key.startsWith("server.")) {
                int dot = key.indexOf('.');
                long sid = Long.parseLong(key.substring(dot + 1));
                QuorumServer qs = new QuorumServer(sid, value);
                allMembers.put(Long.valueOf(sid), qs);  
                if (qs.type == LearnerType.PARTICIPANT) 
                   participatingMembers.put(Long.valueOf(sid), qs);
                else {
                   observingMembers.put(Long.valueOf(sid), qs);
                }
            }
            // 若配置项以“group”开头，则解析配置项，填充成员变量serverGroup
            else if (key.startsWith("group")) {
                int dot = key.indexOf('.');
                long gid = Long.parseLong(key.substring(dot + 1));
                
                numGroups++;
                
                String parts[] = value.split(":");
                for(String s : parts){
                    long sid = Long.parseLong(s);
                    if(serverGroup.containsKey(sid))
                        throw new ConfigException("Server " + sid + "is in multiple groups");
                    else
                        serverGroup.put(sid, gid);
                }
                    
                
            }
            // 若配置项以“weight”开头，则解析配置项，填充成员变量serverWeight
            else if(key.startsWith("weight")) {
                int dot = key.indexOf('.');
                long sid = Long.parseLong(key.substring(dot + 1));
                serverWeight.put(sid, Long.parseLong(value));
            } else if (key.equals("version")){
               version = Long.parseLong(value, 16);
            }        
        }

        // 若有机器不在组里，则抛出异常；若有机器没有权重，则设置权重值1
        for (QuorumServer qs: allMembers.values()){
           Long id = qs.id;
           if (qs.type == LearnerType.PARTICIPANT){
               if (!serverGroup.containsKey(id)) 
                   throw new ConfigException("Server " + id + "is not in a group");
               if (!serverWeight.containsKey(id))
                   serverWeight.put(id, (long) 1);
            }
        }
           
        // 计算服务器组的权重，并存入成员变量groupWeight
        computeGroupWeight();
    }
    
    public Map<Long, QuorumServer> getAllMembers() { 
       return allMembers;
    }

    // 将zk集群中每个机器的ip端口，所属组，权重，输出。
    public String toString(){
       StringWriter sw = new StringWriter();

        // 对每个QuorumServer对象生成一个串 “server.1=192.168.1.11:2888:3888:participant\n”
       for (QuorumServer member: getAllMembers().values()){            
               String key = "server." + member.id;
            String value = member.toString();
            sw.append(key);
            sw.append('=');
            sw.append(value);
            sw.append('\n');                       
       }

       // key是gid，value是该组id下的sid连成的串，形如 "sid1:sid2:sid3"
       Map<Long, String> groups = new HashMap<Long, String>();
       for (Entry<Long, Long> pair: serverGroup.entrySet()) {
           Long sid = pair.getKey();
           Long gid = pair.getValue();
           String str = groups.get(gid);
           if (str == null) str = sid.toString();
           else str = str.concat(":").concat(sid.toString());
           groups.put(gid, str);
       }

       // 组成串类似 "group.1=sid1:sid2:sid3\ngroup.2=sid4:sid5:sid6\n"
       for (Entry<Long, String> pair: groups.entrySet()) {
           Long gid = pair.getKey();
           String key = "group." + gid.toString();
            String value = pair.getValue();
            sw.append(key);
            sw.append('=');
            sw.append(value);
            sw.append('\n');           
       }

        // 组成串类似 "weight.1=1\nweight.2=1\nweight.3=1\n"
       for (Entry<Long, Long> pair: serverWeight.entrySet()) {
           Long sid = pair.getKey();
           String key = "weight." + sid.toString();
            String value = pair.getValue().toString();
            sw.append(key);
            sw.append('=');
            sw.append(value);
            sw.append('\n');           
       }
       
       sw.append("version=" + Long.toHexString(version));
       
       return sw.toString();        
    }
    
    /**
     * This method pre-computes the weights of groups to speed up processing
     * when validating a given set. We compute the weights of groups in 
     * different places, so we have a separate method.
     */
    // 计算服务器组的权重，并存入成员变量groupWeight
    private void computeGroupWeight(){
        // 计算每个组的权重，实际就是该组内机器所占权重的累加和
        for (Entry<Long, Long> entry : serverGroup.entrySet()) {
            Long sid = entry.getKey();
            Long gid = entry.getValue();
            if(!groupWeight.containsKey(gid))
                groupWeight.put(gid, serverWeight.get(sid));
            else {
                long totalWeight = serverWeight.get(sid) + groupWeight.get(gid);
                groupWeight.put(gid, totalWeight);
            }
        }
        
        /*
         * Do not consider groups with weight zero
         * 如果服务器组的权重为0，那么该组不参与投票，将组计数器numGroups减一
         */
        for(long weight: groupWeight.values()){
            LOG.debug("Group weight: " + weight);
            if(weight == ((long) 0)){
                numGroups--;
                LOG.debug("One zero-weight group: " + 1 + ", " + numGroups);
            }
        }
    }
    
    /**
     * Verifies if a given set is a quorum.
     */
    /**
     * 基于权重，判断投票是否通过
     *
     * 就拿本类开头的那段注解举例：
     * 如果是基于多数投票的验证器，（QuorumMaj类）
     *      那么9个zookeeper服务要想集群正常运行的话是需要(9/2 + 1) 也就是5个zookeeper机器正常运行才行的，
     * 如果是基于权重的验证器，（就是本类）
     *      只需要两组sever中各自有两个server运行正常即可，也就是4个zookeeper机器运行正常就能保证集群运行正常。
     *
     *
     * 原则：多数组中的服务器形成多数投票原则
     * 例如有三个分组
     * group.1=1:2:3
     * group.2=4:5:6
     * group.3=7:8:9
     *
     * 默认权重为1，对于这种情况，按照多数原则9个人中超过一半即5个就可以了，但是对于QuorumHierarchical
     * 这种分组，即使超过半数投票也并不一定满足它的原则。
     * 例如，1组投3票，2组1票，3组一票
     * 对于1组，多数成员投票，视为有效组
     * 对于2组，只有1个成员投票，不满足多数投票原则，所以该组视为无效组
     * 3组同理
     * 所以有效组共有1组，不满足多数组原则，选举失败。
     *
     * @param set sid的集合 （sid就是serverid，就是myid文件中的值）
     * @return
     */
    public boolean containsQuorum(Set<Long> set){
        // key是组id，value是该组的权重
        HashMap<Long, Long> expansion = new HashMap<Long, Long>();
        
        /*
         * Adds up weights per group
         */
        if(set.size() == 0) return false;
        else LOG.debug("Set size: " + set.size());

        // 将set中所有sid分成组，并计算每组的权重，填入expansion
        for(long sid : set){
            Long gid = serverGroup.get(sid);
            if (gid == null) continue;
            if(!expansion.containsKey(gid))
                expansion.put(gid, serverWeight.get(sid));
            else {
                long totalWeight = serverWeight.get(sid) + expansion.get(gid);
                expansion.put(gid, totalWeight);
            }
        }
  
        /*
         * Check if all groups have majority
         */
        /**
         *  若上面计算的组的权重值 超过了 groupWeight中对应组的权重值的一半，则将通过投票的组的数量加一
         */
        int majGroupCounter = 0;
        for (Entry<Long, Long> entry : expansion.entrySet()) {
            Long gid = entry.getKey();
            LOG.debug("Group info: {}, {}, {}", entry.getValue(), gid, groupWeight.get(gid));
            // 计算得到的组的权重值 大于groupWeight中该组的权重值的一半。
            if (entry.getValue() > (groupWeight.get(gid) / 2))
                majGroupCounter++;
        }

        // 判断通过投票的组的数量是否超过了组计数器numGroups的一半，如果超过一半，那就表示本轮投票通过
        LOG.debug("Majority group counter: {}, {}", majGroupCounter, numGroups);
        if ((majGroupCounter > (numGroups / 2))){
            LOG.debug("Positive set size: {}", set.size());
            return true;
        } else {
            LOG.debug("Negative set size: {}", set.size());
            return false;
        }
    }
    public Map<Long, QuorumServer> getVotingMembers() {
       return participatingMembers;
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
