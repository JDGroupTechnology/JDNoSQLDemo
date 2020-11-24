#### 客户端使用说明
# JDNoSQLDemo

1. 客户端使用文档
用户可以使用两种方式连接HBase集群,使用Policyserver配置中心和ZK的方式。

Policyserver配置中心的方式支持客户端的无感知平滑主备切换。

ZK连接的方式不支持客户端平滑主备切换,兼容官方客户端方式

Pom.xml 引入依赖：
```
<dependency>
  <groupId>com.jd.bdp.hbase</groupId>
  <artifactId>jdnosql-client</artifactId>
  <version>3.1.4</version>
</dependency>
```
# 1.1. 使用Policyserver配置中心的方式连接hbase：

第一步配置连接串 用户名和密码

第二步配置Policyserver配置中心地址，域名或是服务端列表，二选一

详细连接配置如下jdnosql-client-demo/src/main/resources/hbase-site.xml：

```
<!--3.1.4 使用policyServer配置中心 用户名和密码方式连接 默认使用配置配置中心连接-->
<!--1. 第一步配置连接串 用户名和密码-->
<!--鉴权需要的用户名-->
<property>
  <name>hbase.client.username</name>
  <value>zhangsan</value>
</property>
<!--鉴权需要的密码-->
<property>
  <name>hbase.client.password</name>
  <value>123</value>
</property>

<!--第二步配置Policyserver配置中心地址，域名或是服务端列表，二选一。-->

<!-- 使用ip列表连接Policyserver配置中心-->
<property>
  <name>hbase.policy.http.is.use.domain</name>
  <value>false</value>
</property>
<!--Policyserver配置中心服务地址列表-->
<property>
  <name>hbase.policy.http.server.address</name>
  <value>ip1:16100,ip2:16100</value>
</property>
```
# 1.2. 使用zk方式连接hbase：
第一步配置连接串 用户名和密码
第二步配置zk 连接地址
详细连接配置如下jdnosql-client-demo/src/main/resources/hbase-site.xml：
```
<!--3.1.4 使用zk 用户名和密码方式连接hbase-->
<!--1. 第一步配置连接串 用户名和密码-->

<!--选择zk 方式连接hbase-->
<property>
  <name>hbase.client.connection.impl</name>
  <value>org.apache.hadoop.hbase.client.DefaultClusterConnection</value>
</property>

<!-- 鉴权需要的用户名-->
<property>
  <name>hbase.client.username</name>
  <value>zhangsan</value>
</property>
<!--鉴权需要的密码-->
<property>
  <name>hbase.client.password</name>
  <value>123</value>
</property>
<!--2. 第二步配置zk 连接地址-->
<!--zk 服务列表-->
<property>
  <name>hbase.zookeeper.quorum</name>
  <value>ip1:2181,ip2:2181</value>
</property>
<!--zk node 信息-->
<property>
  <name>zookeeper.znode.parent</name>
  <value>/hbase_xxx</value>
</property>
```
#### 2. 配置项介绍

| 配置|	默认值|	描述  |
|-----|------|------|
|hbase.client.username	|无	|鉴权需要的用户名|
|hbase.client.password	|无	|鉴权需要的密码|
|hbase.policyserver.domain|	无	|配置中心域名，需要根据配置中心真实域名配置|
|hbase.policy.http.is.use.domain|	true	|默认使用域名连接，设置成false使用ip列表连接Policyserver配置中心|
|hbase.policy.http.server.address|	无	|Policyserver配置中心服务地址列表, 如：host1:16100,host2:16100|
