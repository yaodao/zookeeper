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

package org.apache.zookeeper.server.quorum;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.apache.zookeeper.common.AtomicFileWritingIdiom;
import org.apache.zookeeper.common.AtomicFileWritingIdiom.OutputStreamStatement;
import org.apache.zookeeper.common.AtomicFileWritingIdiom.WriterStatement;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.auth.QuorumAuth;
import org.apache.zookeeper.server.quorum.flexible.QuorumHierarchical;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.VerifyingFileFactory;

import static org.apache.zookeeper.common.NetUtils.formatInetAddr;

@InterfaceAudience.Public
public class QuorumPeerConfig {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerConfig.class);
    private static final int UNSET_SERVERID = -1;
    public static final String nextDynamicConfigFileSuffix = ".dynamic.next";

    // 是否允许单机模式（standalone）
    private static boolean standaloneEnabled = true;
    // 是否可以动态更改配置
    private static boolean reconfigEnabled = false;

    protected InetSocketAddress clientPortAddress;
    protected InetSocketAddress secureClientPortAddress;
    protected boolean sslQuorum = false;
    protected boolean shouldUsePortUnification = false;
    protected boolean sslQuorumReloadCertFiles = false;
    protected File dataDir;
    protected File dataLogDir;
    // 动态配置文件的全路径
    protected String dynamicConfigFileStr = null;
    // zoo.cfg的全路径地址
    protected String configFileStr = null;
    // zoo.cfg里配置的tickTime
    protected int tickTime = ZooKeeperServer.DEFAULT_TICK_TIME;
    protected int maxClientCnxns = 60;
    /** defaults to -1 if not set explicitly */
    protected int minSessionTimeout = -1;
    /** defaults to -1 if not set explicitly */
    protected int maxSessionTimeout = -1;
    protected boolean localSessionsEnabled = false;
    protected boolean localSessionsUpgradingEnabled = false;

    protected int initLimit;
    protected int syncLimit;
    protected int electionAlg = 3;
    protected int electionPort = 2182;
    protected boolean quorumListenOnAllIPs = false;

    // 就是myid文件内的值
    protected long serverId = UNSET_SERVERID;

    protected QuorumVerifier quorumVerifier = null, lastSeenQuorumVerifier = null;
    protected int snapRetainCount = 3;
    protected int purgeInterval = 0;
    protected boolean syncEnabled = true;

    // 角色默认是 PARTICIPANT
    protected LearnerType peerType = LearnerType.PARTICIPANT;

    /**
     * Configurations for the quorumpeer-to-quorumpeer sasl authentication
     */
    protected boolean quorumServerRequireSasl = false;
    protected boolean quorumLearnerRequireSasl = false;
    protected boolean quorumEnableSasl = false;
    protected String quorumServicePrincipal = QuorumAuth.QUORUM_KERBEROS_SERVICE_PRINCIPAL_DEFAULT_VALUE;
    protected String quorumLearnerLoginContext = QuorumAuth.QUORUM_LEARNER_SASL_LOGIN_CONTEXT_DFAULT_VALUE;
    protected String quorumServerLoginContext = QuorumAuth.QUORUM_SERVER_SASL_LOGIN_CONTEXT_DFAULT_VALUE;
    protected int quorumCnxnThreadsSize;

    /**
     * Minimum snapshot retain count.
     * @see org.apache.zookeeper.server.PurgeTxnLog#purge(File, File, int)
     */
    private final int MIN_SNAP_RETAIN_COUNT = 3;

    @SuppressWarnings("serial")
    public static class ConfigException extends Exception {
        public ConfigException(String msg) {
            super(msg);
        }
        public ConfigException(String msg, Exception e) {
            super(msg, e);
        }
    }

    /**
     * Parse a ZooKeeper configuration file
     * @param path the patch of the configuration file
     * @throws ConfigException error processing configuration
     */
    public void parse(String path) throws ConfigException {
        LOG.info("Reading configuration from: " + path);
       
        try {
            // 创建File对象，并验证其是否有效 （这里new的是Builder对象， .build()之后才构造了个VerifyingFileFactory对象）
            File configFile = (new VerifyingFileFactory.Builder(LOG)
                .warnForRelativePath()
                .failForNonExistingPath()
                .build()).create(path);
                
            Properties cfg = new Properties();
            FileInputStream in = new FileInputStream(configFile);
            try {
                // 把zoo.cfg的内容读取到Properties对象中
                cfg.load(in);
                configFileStr = path;
            } finally {
                in.close();
            }

            // 从入参cfg中取配置项，赋给当前对象的各个成员变量。
            parseProperties(cfg);
        } catch (IOException e) {
            throw new ConfigException("Error processing " + path, e);
        } catch (IllegalArgumentException e) {
            throw new ConfigException("Error processing " + path, e);
        }   
        
        if (dynamicConfigFileStr!=null) {
           try {           
               Properties dynamicCfg = new Properties();
               FileInputStream inConfig = new FileInputStream(dynamicConfigFileStr);
               try {
                   dynamicCfg.load(inConfig);
                   if (dynamicCfg.getProperty("version") != null) {
                       throw new ConfigException("dynamic file shouldn't have version inside");
                   }

                   String version = getVersionFromFilename(dynamicConfigFileStr);
                   // If there isn't any version associated with the filename,
                   // the default version is 0.
                   if (version != null) {
                       dynamicCfg.setProperty("version", version);
                   }
               } finally {
                   inConfig.close();
               }
               setupQuorumPeerConfig(dynamicCfg, false);

           } catch (IOException e) {
               throw new ConfigException("Error processing " + dynamicConfigFileStr, e);
           } catch (IllegalArgumentException e) {
               throw new ConfigException("Error processing " + dynamicConfigFileStr, e);
           }        
           File nextDynamicConfigFile = new File(configFileStr + nextDynamicConfigFileSuffix);
           if (nextDynamicConfigFile.exists()) {
               try {           
                   Properties dynamicConfigNextCfg = new Properties();
                   FileInputStream inConfigNext = new FileInputStream(nextDynamicConfigFile);       
                   try {
                       dynamicConfigNextCfg.load(inConfigNext);
                   } finally {
                       inConfigNext.close();
                   }
                   boolean isHierarchical = false;
                   for (Entry<Object, Object> entry : dynamicConfigNextCfg.entrySet()) {
                       String key = entry.getKey().toString().trim();  
                       if (key.startsWith("group") || key.startsWith("weight")) {
                           isHierarchical = true;
                           break;
                       }
                   }
                   lastSeenQuorumVerifier = createQuorumVerifier(dynamicConfigNextCfg, isHierarchical);
               } catch (IOException e) {
                   LOG.warn("NextQuorumVerifier is initiated to null");
               }
           }
        }
    }

    // This method gets the version from the end of dynamic file name.
    // For example, "zoo.cfg.dynamic.0" returns initial version "0".
    // "zoo.cfg.dynamic.1001" returns version of hex number "0x1001".
    // If a dynamic file name doesn't have any version at the end of file,
    // e.g. "zoo.cfg.dynamic", it returns null.
    public static String getVersionFromFilename(String filename) {
        int i = filename.lastIndexOf('.');
        if(i < 0 || i >= filename.length())
            return null;

        String hexVersion = filename.substring(i + 1);
        try {
            long version = Long.parseLong(hexVersion, 16);
            return Long.toHexString(version);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Parse config from a Properties.
     * @param zkProp Properties to parse from.
     * @throws IOException
     * @throws ConfigException
     */
    // 从入参zkProp中取配置值，赋给当前对象的各个成员变量。
    public void parseProperties(Properties zkProp)
    throws IOException, ConfigException {
        int clientPort = 0;
        int secureClientPort = 0;
        String clientPortAddress = null;
        String secureClientPortAddress = null;
        VerifyingFileFactory vff = new VerifyingFileFactory.Builder(LOG).warnForRelativePath().build();
        for (Entry<Object, Object> entry : zkProp.entrySet()) {
            String key = entry.getKey().toString().trim();
            String value = entry.getValue().toString().trim();
            if (key.equals("dataDir")) {
                dataDir = vff.create(value);
            } else if (key.equals("dataLogDir")) {
                dataLogDir = vff.create(value);
            } else if (key.equals("clientPort")) {
                clientPort = Integer.parseInt(value);
            } else if (key.equals("localSessionsEnabled")) {
                localSessionsEnabled = Boolean.parseBoolean(value);
            } else if (key.equals("localSessionsUpgradingEnabled")) {
                localSessionsUpgradingEnabled = Boolean.parseBoolean(value);
            }
            /**
             * 对于多网卡的机器，可以为每个IP指定不同的监听端口。默认情况是所有IP都监听 clientPort指定的端口。
             * 限制客户端连接到指定的接收信息的地址上。
             * 默认情况下，一个zookeeper服务器会监听在所有的网络接口地址上等待客户端的连接。
             * 有些服务器配置了多个网络接口，其中一个网络接口用于内网通信，另一个网络接口用于公网通信，
             * 如果你并不希望服务器在公网接口接受客户端的连接，只需要设置clientPortAddress选项为内网接口的地址。
             */
            else if (key.equals("clientPortAddress")) {
                clientPortAddress = value.trim();
            } else if (key.equals("secureClientPort")) {
                secureClientPort = Integer.parseInt(value);
            } else if (key.equals("secureClientPortAddress")){
                secureClientPortAddress = value.trim();
            }
            // ZK中的一个时间单元。ZK中所有时间都是以这个时间单元为基础，进行整数倍的配置。例如，session的最小超时时间是2*tickTime。
            else if (key.equals("tickTime")) {
                tickTime = Integer.parseInt(value);
            }
            /**
             * 单个客户端与单台服务器之间的连接数的限制，是ip级别的，默认是60，如果设置为0，那么表明不作任何限制。
             * 请注意这个限制的使用范围，仅仅是单个客户端与单台ZK服务器之间的连接数限制，
             * 不是针对指定客户端IP，也不是ZK集群的连接数限制，也不是单台ZK对所有客户端的连接数限制。
             */
            else if (key.equals("maxClientCnxns")) {
                maxClientCnxns = Integer.parseInt(value);
            }
            // Session超时时间限制，如果客户端设置的超时时间不在这个范围，那么会被强制设置为最大或最小时间范围内。
            // 默认的Session超时时间是在2 * tickTime ~ 20 * tickTime 这个范围
            else if (key.equals("minSessionTimeout")) {
                minSessionTimeout = Integer.parseInt(value);
            } else if (key.equals("maxSessionTimeout")) {
                maxSessionTimeout = Integer.parseInt(value);
            }
            /**
             * Follower在启动过程中，会从Leader同步所有最新数据，然后确定自己能够对外服务的起始状态。
             * Leader允许Follower在initLimit时间内完成这个工作。
             * 通常情况下，我们不用太在意这个参数的设置。
             * 如果ZK集群的数据量确实很大了，Follower在启动的时候，从Leader上同步数据的时间也会相应变长，
             * 因此在这种情况下，有必要适当调大这个参数了。
             */
            else if (key.equals("initLimit")) {
                initLimit = Integer.parseInt(value);
            }
            /**
             * 在运行过程中，Leader负责与ZK集群中所有机器进行通信，例如通过一些心跳检测机制，来检测机器的存活状态。
             * 如果Leader发出心跳包在syncLimit之后，还没有从Follower那里收到响应，那么就认为这个Follower已经不在线了。
             * 注意：不要把这个参数设置得过大，否则可能会掩盖一些问题。
             */
            else if (key.equals("syncLimit")) {
                syncLimit = Integer.parseInt(value);
            }
            // leader选举算法，只有一种算法“TCP-based version of fast leader election”
            else if (key.equals("electionAlg")) {
                electionAlg = Integer.parseInt(value);
            } else if (key.equals("quorumListenOnAllIPs")) {
                quorumListenOnAllIPs = Boolean.parseBoolean(value);
            } else if (key.equals("peerType")) {
                if (value.toLowerCase().equals("observer")) {
                    peerType = LearnerType.OBSERVER;
                } else if (value.toLowerCase().equals("participant")) {
                    peerType = LearnerType.PARTICIPANT;
                } else
                {
                    throw new ConfigException("Unrecognised peertype: " + value);
                }
            } else if (key.equals( "syncEnabled" )) {
                syncEnabled = Boolean.parseBoolean(value);
            }
            // 动态配置文件
            else if (key.equals("dynamicConfigFile")){
                dynamicConfigFileStr = value;
            }
            // 指定了需要保留的文件数目。默认是保留3个
            else if (key.equals("autopurge.snapRetainCount")) {
                snapRetainCount = Integer.parseInt(value);
            }
            // ZK提供了自动清理事务日志和快照文件的功能，这个参数指定了清理频率，单位是小时，需要配置一个1或更大的整数，默认是0，表示不开启自动清理功能。
            else if (key.equals("autopurge.purgeInterval")) {
                purgeInterval = Integer.parseInt(value);
            } else if (key.equals("standaloneEnabled")) {
                if (value.toLowerCase().equals("true")) {
                    setStandaloneEnabled(true);
                } else if (value.toLowerCase().equals("false")) {
                    setStandaloneEnabled(false);
                } else {
                    throw new ConfigException("Invalid option " + value + " for standalone mode. Choose 'true' or 'false.'");
                }
            }
            // 是否可以动态更改配置
            else if (key.equals("reconfigEnabled")) {
                if (value.toLowerCase().equals("true")) {
                    setReconfigEnabled(true);
                } else if (value.toLowerCase().equals("false")) {
                    setReconfigEnabled(false);
                } else {
                    throw new ConfigException("Invalid option " + value + " for reconfigEnabled flag. Choose 'true' or 'false.'");
                }
            } else if (key.equals("sslQuorum")){
                sslQuorum = Boolean.parseBoolean(value);
            } else if (key.equals("portUnification")){
                shouldUsePortUnification = Boolean.parseBoolean(value);
            } else if (key.equals("sslQuorumReloadCertFiles")) {
                sslQuorumReloadCertFiles = Boolean.parseBoolean(value);
            } else if ((key.startsWith("server.") || key.startsWith("group") || key.startsWith("weight")) && zkProp.containsKey("dynamicConfigFile")) {
                throw new ConfigException("parameter: " + key + " must be in a separate dynamic config file");
            } else if (key.equals(QuorumAuth.QUORUM_SASL_AUTH_ENABLED)) {
                quorumEnableSasl = Boolean.parseBoolean(value);
            } else if (key.equals(QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED)) {
                quorumServerRequireSasl = Boolean.parseBoolean(value);
            } else if (key.equals(QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED)) {
                quorumLearnerRequireSasl = Boolean.parseBoolean(value);
            } else if (key.equals(QuorumAuth.QUORUM_LEARNER_SASL_LOGIN_CONTEXT)) {
                quorumLearnerLoginContext = value;
            } else if (key.equals(QuorumAuth.QUORUM_SERVER_SASL_LOGIN_CONTEXT)) {
                quorumServerLoginContext = value;
            } else if (key.equals(QuorumAuth.QUORUM_KERBEROS_SERVICE_PRINCIPAL)) {
                quorumServicePrincipal = value;
            } else if (key.equals("quorum.cnxn.threads.size")) {
                quorumCnxnThreadsSize = Integer.parseInt(value);
            } else {
                // 将配置项设置到系统属性中。
                System.setProperty("zookeeper." + key, value);
            }
        }
        // 以上就将zoo.cfg中的配置的值赋值给了当前对象的成员变量。

        // 以下是验证，解析配置文件得到的成员变量值是否有效。

        if (!quorumEnableSasl && quorumServerRequireSasl) {
            throw new IllegalArgumentException(
                    QuorumAuth.QUORUM_SASL_AUTH_ENABLED
                            + " is disabled, so cannot enable "
                            + QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED);
        }
        if (!quorumEnableSasl && quorumLearnerRequireSasl) {
            throw new IllegalArgumentException(
                    QuorumAuth.QUORUM_SASL_AUTH_ENABLED
                            + " is disabled, so cannot enable "
                            + QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED);
        }
        // If quorumpeer learner is not auth enabled then self won't be able to
        // join quorum. So this condition is ensuring that the quorumpeer learner
        // is also auth enabled while enabling quorum server require sasl.
        if (!quorumLearnerRequireSasl && quorumServerRequireSasl) {
            throw new IllegalArgumentException(
                    QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED
                            + " is disabled, so cannot enable "
                            + QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED);
        }

        // Reset to MIN_SNAP_RETAIN_COUNT if invalid (less than 3)
        // PurgeTxnLog.purge(File, File, int) will not allow to purge less
        // than 3.
        // snapRetainCount最小是3
        if (snapRetainCount < MIN_SNAP_RETAIN_COUNT) {
            LOG.warn("Invalid autopurge.snapRetainCount: " + snapRetainCount
                    + ". Defaulting to " + MIN_SNAP_RETAIN_COUNT);
            snapRetainCount = MIN_SNAP_RETAIN_COUNT;
        }

        if (dataDir == null) {
            throw new IllegalArgumentException("dataDir is not set");
        }
        if (dataLogDir == null) {
            dataLogDir = dataDir;
        }

        if (clientPort == 0) {
            LOG.info("clientPort is not set");
            if (clientPortAddress != null) {
                throw new IllegalArgumentException("clientPortAddress is set but clientPort is not set");
            }
        }
        //  若配置文件中配置了clientPortAddress，则取它的ip地址 与clientPort一起构成一个InetSocketAddress对象
        else if (clientPortAddress != null) {
            this.clientPortAddress = new InetSocketAddress(
                    InetAddress.getByName(clientPortAddress), clientPort);
            LOG.info("clientPortAddress is {}", formatInetAddr(this.clientPortAddress));
        }
        // 若配置文件中配置了clientPort，但没有配置clientPortAddress
        else {
            // 生成的InetSocketAddress对象，ip=0.0.0.0 端口是clientPort
            this.clientPortAddress = new InetSocketAddress(clientPort);
            LOG.info("clientPortAddress is {}", formatInetAddr(this.clientPortAddress));
        }

        if (secureClientPort == 0) {
            LOG.info("secureClientPort is not set");
            if (secureClientPortAddress != null) {
                throw new IllegalArgumentException("secureClientPortAddress is set but secureClientPort is not set");
            }
        } else if (secureClientPortAddress != null) {
            this.secureClientPortAddress = new InetSocketAddress(
                    InetAddress.getByName(secureClientPortAddress), secureClientPort);
            LOG.info("secureClientPortAddress is {}", formatInetAddr(this.secureClientPortAddress));
        } else {
            this.secureClientPortAddress = new InetSocketAddress(secureClientPort);
            LOG.info("secureClientPortAddress is {}", formatInetAddr(this.secureClientPortAddress));
        }
        if (this.secureClientPortAddress != null) {
            configureSSLAuth();
        }

        if (tickTime == 0) {
            throw new IllegalArgumentException("tickTime is not set");
        }

        // session的默认最小和最大时间。
        minSessionTimeout = minSessionTimeout == -1 ? tickTime * 2 : minSessionTimeout;
        maxSessionTimeout = maxSessionTimeout == -1 ? tickTime * 20 : maxSessionTimeout;

        if (minSessionTimeout > maxSessionTimeout) {
            throw new IllegalArgumentException(
                    "minSessionTimeout must not be larger than maxSessionTimeout");
        }          

        // backward compatibility - dynamic configuration in the same file as
        // static configuration params see writeDynamicConfig()
        // 向后兼容性——与静态配置参数在同一个文件中的动态配置参见writeDynamicConfig()
        if (dynamicConfigFileStr == null) {
            setupQuorumPeerConfig(zkProp, true);
            // 集群 且 可以动态更改配置，则备份文件。
            if (isDistributed() && isReconfigEnabled()) {
                // we don't backup static config for standalone mode.
                // we also don't backup if reconfig feature is disabled.
                // standalone模式不备份文件， reconfigEnabled = false 也不备份文件。
                // 备份文件
                backupOldConfig();
            }
        }
    }

    /**
     * Configure SSL authentication only if it is not configured.
     * 
     * @throws ConfigException
     *             If authentication scheme is configured but authentication
     *             provider is not configured.
     */
    public static void configureSSLAuth() throws ConfigException {
        try (ClientX509Util clientX509Util = new ClientX509Util()) {
            String sslAuthProp = "zookeeper.authProvider." + System.getProperty(clientX509Util.getSslAuthProviderProperty(), "x509");
            if (System.getProperty(sslAuthProp) == null) {
                if ("zookeeper.authProvider.x509".equals(sslAuthProp)) {
                    System.setProperty("zookeeper.authProvider.x509",
                            "org.apache.zookeeper.server.auth.X509AuthenticationProvider");
                } else {
                    throw new ConfigException("No auth provider configured for the SSL authentication scheme '"
                            + System.getProperty(clientX509Util.getSslAuthProviderProperty()) + "'.");
                }
            }
        }
    }

    /**
     * Backward compatibility -- It would backup static config file on bootup
     * if users write dynamic configuration in "zoo.cfg".
     */
    /**
     * 向后兼容性——如果用户在“zoo.cfg”中 设置了启用动态配置reconfigEnabled = true，则在启动时备份静态配置文件。
     */
    private void backupOldConfig() throws IOException {
        // 这个new对象的过程中，就把configFileStr对应的文件的内容 写入到了configFileStr.bak中（实现了文件的备份）
        new AtomicFileWritingIdiom(new File(configFileStr + ".bak"), new OutputStreamStatement() {
            @Override
            public void write(OutputStream output) throws IOException {
                InputStream input = null;
                try {
                    input = new FileInputStream(new File(configFileStr));
                    byte[] buf = new byte[1024];
                    int bytesRead;
                    while ((bytesRead = input.read(buf)) > 0) {
                        output.write(buf, 0, bytesRead);
                    }
                } finally {
                    if( input != null) {
                        input.close();
                    }
                }
            }
        });
    }


    /**
     * Writes dynamic configuration file
     *
     *  将qv中的信息，写入到dynamicConfigFilename文件中
     *
     * @param dynamicConfigFilename  生成的新文件
     * @param qv 信息来源
     * @param needKeepVersion
     * @throws IOException
     */
    public static void writeDynamicConfig(final String dynamicConfigFilename,
                                          final QuorumVerifier qv,
                                          final boolean needKeepVersion)
            throws IOException {

        new AtomicFileWritingIdiom(new File(dynamicConfigFilename), new WriterStatement() {
            @Override
            public void write(Writer out) throws IOException {
                Properties cfg = new Properties();
                // qv串就是 zk集群中每个机器的ip:port:type用"\n"连接在一起
                cfg.load( new StringReader(
                        qv.toString()));

                // 将所有类似 "server.1=192.168.1.11:2888:3888" 这种配置写入out流
                List<String> servers = new ArrayList<String>();
                for (Entry<Object, Object> entry : cfg.entrySet()) {
                    String key = entry.getKey().toString().trim();
                    if ( !needKeepVersion && key.startsWith("version"))
                        continue;

                    String value = entry.getValue().toString().trim();
                    servers.add(key
                            .concat("=")
                            .concat(value));
                }

                Collections.sort(servers);
                out.write(StringUtils.joinStrings(servers, "\n"));
            }
        });
    }

    /**
     * Edit static config file.
     * If there are quorum information in static file, e.g. "server.X", "group",
     * it will remove them.
     * If it needs to erase client port information left by the old config,
     * "eraseClientPortAddress" should be set true.
     * It should also updates dynamic file pointer on reconfig.
     */
    /**
     * 编辑静态配置文件。
     * 如果在静态配置文件中有信息如：“server.X"， "group"，则删除它们。
     * 如果需要删除静态配置文件中的client port信息，则“eraseClientPortAddress”应该设置为true。
     * 更新静态配置文件中dynamicConfigFile的值。
     */
    public static void editStaticConfig(final String configFileStr,
                                        final String dynamicFileStr,
                                        final boolean eraseClientPortAddress)
            throws IOException {
        // Some tests may not have a static config file.
        if (configFileStr == null)
            return;

        File configFile = (new VerifyingFileFactory.Builder(LOG)
                .warnForRelativePath()
                .failForNonExistingPath()
                .build()).create(configFileStr);

        final File dynamicFile = (new VerifyingFileFactory.Builder(LOG)
                .warnForRelativePath()
                .failForNonExistingPath()
                .build()).create(dynamicFileStr);
        
        final Properties cfg = new Properties();
        FileInputStream in = new FileInputStream(configFile);
        try {
            cfg.load(in);
        } finally {
            in.close();
        }

        new AtomicFileWritingIdiom(new File(configFileStr), new WriterStatement() {
            @Override
            public void write(Writer out) throws IOException {
                for (Entry<Object, Object> entry : cfg.entrySet()) {
                    String key = entry.getKey().toString().trim();

                    if (key.startsWith("server.")
                        || key.startsWith("group")
                        || key.startsWith("weight")
                        || key.startsWith("dynamicConfigFile")
                        || (eraseClientPortAddress
                            && (key.startsWith("clientPort")
                                || key.startsWith("clientPortAddress")))) {
                        // not writing them back to static file
                        // 这些配置不再写到静态配置文件中。
                        continue;
                    }

                    String value = entry.getValue().toString().trim();
                    out.write(key.concat("=").concat(value).concat("\n"));
                }

                // updates the dynamic file pointer
                // 将动态配置文件的路径写入out流
                String dynamicConfigFilePath = PathUtils.normalizeFileSystemPath(dynamicFile.getCanonicalPath());
                out.write("dynamicConfigFile="
                         .concat(dynamicConfigFilePath)
                         .concat("\n"));
            }
        });
    }


    // 删文件或者空目录（非空目录删不掉，也不抛出异常，就是调用delete后删不掉，试过）
    public static void deleteFile(String filename){
       if (filename == null) return;
       File f = new File(filename);
       if (f.exists()) {
           try{ 
               f.delete();
           } catch (Exception e) {
               LOG.warn("deleting " + filename + " failed");
           }
       }                   
    }

    /**
     * 创建一个投票验证器
     * 现在QuorumHierarchical层级验证器都不用了，只用QuorumMaj验证器
     */
    private static QuorumVerifier createQuorumVerifier(Properties dynamicConfigProp, boolean isHierarchical) throws ConfigException{
       if(isHierarchical){
            return new QuorumHierarchical(dynamicConfigProp);
        } else {
           /*
             * The default QuorumVerifier is QuorumMaj
             */        
            //LOG.info("Defaulting to majority quorums");
            return new QuorumMaj(dynamicConfigProp);            
        }          
    }

    // 设置当前对象的成员变量 quorumVerifier，serverId，clientPortAddress，peerType
    void setupQuorumPeerConfig(Properties prop, boolean configBackwardCompatibilityMode)
            throws IOException, ConfigException {
        // 用入参prop中的配置项，构造一个投票验证器，赋值给成员变量quorumVerifier
        quorumVerifier = parseDynamicConfig(prop, electionAlg, true, configBackwardCompatibilityMode);
        // 给成员变量serverId赋值，就是文件myid的内容
        setupMyId();
        setupClientPort();
        setupPeerType();
        checkValidity();
    }

    /**
     * Parse dynamic configuration file and return
     * quorumVerifier for new configuration.
     * @param dynamicConfigProp Properties to parse from.
     * @throws IOException
     * @throws ConfigException
     */
    // 用入参dynamicConfigProp中的配置项，构造一个投票验证器，并检验它的属性值后，返回。
    public static QuorumVerifier parseDynamicConfig(Properties dynamicConfigProp, int eAlg, boolean warnings,
	   boolean configBackwardCompatibilityMode) throws IOException, ConfigException {
       boolean isHierarchical = false;
        /**
         * 看看配置的是不是层级验证器
         * QuorumHierarchical层级验证器现在已经不用了，默认就是QuorumMaj验证器
         */
        for (Entry<Object, Object> entry : dynamicConfigProp.entrySet()) {
            String key = entry.getKey().toString().trim();                    
            if (key.startsWith("group") || key.startsWith("weight")) {
               isHierarchical = true;
            } else if (!configBackwardCompatibilityMode && !key.startsWith("server.") && !key.equals("version")){ 
               LOG.info(dynamicConfigProp.toString());
               throw new ConfigException("Unrecognised parameter: " + key);                
            }
        }

        // 创建一个投票验证器，同时该验证器的成员变量也被赋上值了。
        QuorumVerifier qv = createQuorumVerifier(dynamicConfigProp, isHierarchical);
               
        int numParticipators = qv.getVotingMembers().size();
        int numObservers = qv.getObservingMembers().size();
        // 对participant的数量做一个校验
        if (numParticipators == 0) {
            // participant数量是0，若不是单机模式，则抛出异常。
            if (!standaloneEnabled) {
                throw new IllegalArgumentException("standaloneEnabled = false then " +
                        "number of participants should be >0");
            }
            if (numObservers > 0) {
                // 只有observer没有participant 是无效的配置。
                throw new IllegalArgumentException("Observers w/o participants is an invalid configuration");
            }
        } else if (numParticipators == 1 && standaloneEnabled) {
            // HBase currently adds a single server line to the config, for
            // b/w compatibility reasons we need to keep this here. If standaloneEnabled
            // is true, the QuorumPeerMain script will create a standalone server instead
            // of a quorum configuration
            LOG.error("Invalid configuration, only one server specified (ignoring)");
            if (numObservers > 0) {
                throw new IllegalArgumentException("Observers w/o quorum is an invalid configuration");
            }
        } else {
            if (warnings) {
                if (numParticipators <= 2) {
                    LOG.warn("No server failure will be tolerated. " +
                        "You need at least 3 servers.");
                } else if (numParticipators % 2 == 0) {
                    // 非最优配置，考虑奇数个服务器
                    LOG.warn("Non-optimial configuration, consider an odd number of servers.");
                }
            }
            /*
             * If using FLE, then every server requires a separate election
             * port.
             * FLE（Fast Leader Election）
             */            
           if (eAlg != 0) {
               // 所有参加选举的机器，都得有ip和选举用的端口，否则抛出异常。（配置得是这样： server.1=192.168.1.11:2888:3888）
               for (QuorumServer s : qv.getVotingMembers().values()) {
                   if (s.electionAddr == null)
                       throw new IllegalArgumentException(
                               "Missing election port for server: " + s.id);
               }
           }   
        }
        return qv;
    }

    // 读取文件myid的内容，赋值给成员变量serverId
    private void setupMyId() throws IOException {
        File myIdFile = new File(dataDir, "myid");
        // standalone server doesn't need myid file.
        if (!myIdFile.isFile()) {
            return;
        }
        BufferedReader br = new BufferedReader(new FileReader(myIdFile));
        String myIdString;
        try {
            myIdString = br.readLine();
        } finally {
            br.close();
        }
        try {
            serverId = Long.parseLong(myIdString);
            MDC.put("myid", myIdString);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("serverid " + myIdString
                    + " is not a number");
        }
    }

    // 给成员变量clientPortAddress赋值
    private void setupClientPort() throws ConfigException {
        if (serverId == UNSET_SERVERID) {
            return;
        }
        QuorumServer qs = quorumVerifier.getAllMembers().get(serverId);
        // 这段感觉意思是： 不能在动态配置文件中更改静态配置文件中client的地址和端口，否则抛出异常。
        if (clientPortAddress != null && qs != null && qs.clientAddr != null) {
            // 若clientPortAddress!=0.0.0.0 但是ip地址不同，则抛出异常
            // 若clientPortAddress==0.0.0.0 但是端口不同，则抛出异常
            if ((!clientPortAddress.getAddress().isAnyLocalAddress()
                    && !clientPortAddress.equals(qs.clientAddr)) ||
                    (clientPortAddress.getAddress().isAnyLocalAddress()
                            && clientPortAddress.getPort() != qs.clientAddr.getPort()))
                // 从这句可以看出， 当前对象的成员变量是从静态配置文件中读取， qs变量的属性是从动态配置文件中读取
                throw new ConfigException("client address for this server (id = " + serverId +
                        ") in static config file is " + clientPortAddress +
                        " is different from client address found in dynamic file: " + qs.clientAddr);
        }
        if (qs != null && qs.clientAddr != null) clientPortAddress = qs.clientAddr;
    }

    // 设置成员变量peerType
    private void setupPeerType() {
        // Warn about inconsistent peer type
        LearnerType roleByServersList = quorumVerifier.getObservingMembers().containsKey(serverId) ? LearnerType.OBSERVER
                : LearnerType.PARTICIPANT;
        if (roleByServersList != peerType) {
            LOG.warn("Peer type from servers list (" + roleByServersList
                    + ") doesn't match peerType (" + peerType
                    + "). Defaulting to servers list.");

            peerType = roleByServersList;
        }
    }

    // 检查几个必要的配置项
    public void checkValidity() throws IOException, ConfigException{
        if (isDistributed()) {
            if (initLimit == 0) {
                throw new IllegalArgumentException("initLimit is not set");
            }
            if (syncLimit == 0) {
                throw new IllegalArgumentException("syncLimit is not set");
            }
            if (serverId == UNSET_SERVERID) {
                throw new IllegalArgumentException("myid file is missing");
            }
       }
    }

    public InetSocketAddress getClientPortAddress() { return clientPortAddress; }
    public InetSocketAddress getSecureClientPortAddress() { return secureClientPortAddress; }
    public File getDataDir() { return dataDir; }
    public File getDataLogDir() { return dataLogDir; }
    public int getTickTime() { return tickTime; }
    public int getMaxClientCnxns() { return maxClientCnxns; }
    public int getMinSessionTimeout() { return minSessionTimeout; }
    public int getMaxSessionTimeout() { return maxSessionTimeout; }
    public boolean areLocalSessionsEnabled() { return localSessionsEnabled; }
    public boolean isLocalSessionsUpgradingEnabled() {
        return localSessionsUpgradingEnabled;
    }
    public boolean isSslQuorum() {
        return sslQuorum;
    }

    public boolean shouldUsePortUnification() {
        return shouldUsePortUnification;
    }

    public int getInitLimit() { return initLimit; }
    public int getSyncLimit() { return syncLimit; }
    public int getElectionAlg() { return electionAlg; }
    public int getElectionPort() { return electionPort; }

    public int getSnapRetainCount() {
        return snapRetainCount;
    }

    public int getPurgeInterval() {
        return purgeInterval;
    }
    
    public boolean getSyncEnabled() {
        return syncEnabled;
    }

    public QuorumVerifier getQuorumVerifier() {
        return quorumVerifier;
    }
    
    public QuorumVerifier getLastSeenQuorumVerifier() {   
        return lastSeenQuorumVerifier;
    }

    public Map<Long,QuorumServer> getServers() {
        // returns all configuration servers -- participants and observers
        return Collections.unmodifiableMap(quorumVerifier.getAllMembers());
    }

    public long getServerId() { return serverId; }

    // 是否集群，true是
    public boolean isDistributed() {
        return quorumVerifier!=null && (!standaloneEnabled || quorumVerifier.getVotingMembers().size() > 1);
    }

    public LearnerType getPeerType() {
        return peerType;
    }

    public String getConfigFilename(){
        return configFileStr;
    }
    
    public Boolean getQuorumListenOnAllIPs() {
        return quorumListenOnAllIPs;
    }
 
    public static boolean isStandaloneEnabled() {
	return standaloneEnabled;
    }
    
    public static void setStandaloneEnabled(boolean enabled) {
        standaloneEnabled = enabled;
    }

    public static boolean isReconfigEnabled() { return reconfigEnabled; }

    public static void setReconfigEnabled(boolean enabled) {
        reconfigEnabled = enabled;
    }

}
