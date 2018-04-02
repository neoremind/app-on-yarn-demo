# Service oriented application hosted on YARN demo

## 1. 介绍
[Apache Hadoop Yarn](http://hadoop.apache.org/)是big data领域通用的资源管理与调度平台，很多计算框架均可以跑在Yarn上，例如Mapreduce、Spark、Flink、Storm等，这些计算框架可以专注于计算本身，Yarn提供的高度抽象的接口来做集成。

![https://github.com/neoremind/mydoc/blob/master/image/yarn_arch.png](https://github.com/neoremind/mydoc/blob/master/image/yarn_arch.png)

除了big data以外，实际一些长服务（long time running service）也可以跑在Yarn上，这里做了一些探索。这个项目就可以把service跑在Yarn上，一些实际场景例如，需要考虑HDFS本地化的OLAP引擎，实际生产环境的例子是[Hulu的OLAP引擎Nesto](http://neoremind.com/2018/03/nesto-hulu%E7%94%A8%E6%88%B7%E5%88%86%E6%9E%90%E5%B9%B3%E5%8F%B0%E7%9A%84olap%E5%BC%95%E6%93%8E/)跑在Yarn上面；或者干脆就是一个纯粹的service。总之，和big data亲缘性比较高的项目适合Yarn，作为和k8s等的一种补充。

如下图，`service x`通过Yarn维持固定跑2个实例。


![https://github.com/neoremind/mydoc/blob/master/image/yarn_service.png](https://github.com/neoremind/mydoc/blob/master/image/yarn_service.png)

另外，[Twill](http://twill.apache.org/)是一个基于Yarn抽象出来的编程模型组件，基于其API可以很方便的开发托管Yarn的分布式程序。本项目的程序可以看做是教学和简单版本的Twill，并不是一个成熟的开源组件。

## 2. 搭建Hadoop环境
必备环境包括HDFS、YARN。

如果读者已经有了Hadoop环境，请跳过此节。如果没有，可以参考下面的docker环境本地搭建。
[https://github.com/neoremind/hadoop-cluster-docker](https://github.com/neoremind/hadoop-cluster-docker)

## 3. 代码说明

为支持通过YARN做资源管理、调度以及应用高可用保证，可以参考下面的类，做定制。

| 类名    | 路径      | 作用     | 
| -------------- | ------------ | -------------- |
| SampleExecutor            | [Link](https://github.com/neoremind/app-on-yarn-demo/blob/dev/src/main/java/com/neoremind/app/on/yarn/demo/SampleExecutor.java)      |  用户自定义的执行主引擎，例如上述的跑一个HTTP服务，YARN会在container中跑这个类       |
| SampleHttpServer            | [Link](https://github.com/neoremind/app-on-yarn-demo/blob/dev/src/main/java/com/neoremind/app/on/yarn/demo/SampleHttpServer.java)      |  用户自定义的一个例子，集成Jetty       |
| Constants            | [Link](https://github.com/neoremind/app-on-yarn-demo/blob/dev/src/main/java/com/neoremind/app/on/yarn/demo/Constants.java)      |  包括静态变量，YARN Container跑Java类的main class       |

另外两个重要的类如下，无需修改。

| 类名    | 路径      | 作用     | 
| -------------- | ------------ | -------------- |
| ApplicationMaster            | [Link](https://github.com/neoremind/app-on-yarn-demo/blob/dev/src/main/java/com/neoremind/app/on/yarn/demo/ApplicationMaster.java)      |  YARN ApplicationMaster程序       |
| Client            | [Link](https://github.com/neoremind/app-on-yarn-demo/blob/dev/src/main/java/com/neoremind/app/on/yarn/demo/Client.java)      |  YARN Client程序       |

Client是用于提交YARN上运行程序的入口，通过命令行其参数如下。下面的demo **4.5**中会有例子。
```
usage:
 -appname <arg>                                 Application Name. Default
                                                value - DistributedShell
 -container_memory <arg>                        Amount of memory in MB to
                                                be requested to run the
                                                shell command
 -container_vcores <arg>                        Amount of virtual cores to
                                                be requested to run the
                                                shell command
 -debug                                         Dump out debug information
 -help                                          Print usage
 -jar_path <arg>                                Jar file containing the
                                                application master in
                                                local file system
 -jar_path_in_hdfs <arg>                        Jar file containing the
                                                application master in HDFS
 -java_opts <arg>                               Java opts for container
 -keep_containers_across_application_attempts   Flag to indicate whether
                                                to keep containers across
                                                application attempts. If
                                                the flag is true, running
                                                containers will not be
                                                killed when application
                                                attempt fails and these
                                                containers will be
                                                retrieved by the new
                                                application attempt
 -log_properties <arg>                          log4j.properties file
 -master_memory <arg>                           Amount of memory in MB to
                                                be requested to run the
                                                application master
 -master_vcores <arg>                           Amount of virtual cores to
                                                be requested to run the
                                                application master
 -memory_overhead <arg>                         Amount of memory overhead
                                                in MB for application
                                                master and container
 -num_containers <arg>                          No. of containers on which
                                                the shell command needs to
                                                be executed
 -priority <arg>                                Application Priority.
                                                Default 0
 -queue <arg>                                   RM Queue in which this
                                                application is to be
                                                submitted
 -shell_args <arg>                              Command line args for the
                                                shell script.Multiple args
                                                can be separated by empty
                                                space.
 -shell_env <arg>                               Environment for shell
                                                script. Specified as
                                                env_key=env_val pairs
 -timeout <arg>                                 Application timeout in
                                                milliseconds
```

其他类无需修改。

## 4. 运行demo

### 运行demo动图

![](https://github.com/neoremind/mydoc/blob/master/image/yarn-demo4.gif)

### 高可用测试动图

![](https://github.com/neoremind/mydoc/blob/master/image/yarn-demo5.gif)

注意，确保`yarn-site.xml`中的配置，是一个比较大的值，否则只有container支持高可用，app master默认挂2次就会退出，例如设置成99.
```
<property>
  <name>yarn.resourcemanager.am.max-attempts</name>
  <value>99</value>
</property>
```

### 4.1 启动Hadoop HDFS, YARN
参考[https://github.com/neoremind/hadoop-cluster-docker](https://github.com/neoremind/hadoop-cluster-docker)完成前8步。

### 4.2 编译打包
在本地
```
mvn clean package
```
输出在/YOUR_PATH/target/app-on-yarn-demo-1.0.0-SNAPSHOT.jar

### 4.3 拷贝JAR包到hadoop-master容器内
在本地，确认`hadoop-master`的container id，如下为`c5c094753715`。
```
docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED              STATUS              PORTS                                                                                        NAMES
e3f5ad6ebac0        xuzh/hadoop:1.0     "sh -c 'service ssh s"   About a minute ago   Up About a minute   0.0.0.0:8042->8042/tcp, 0.0.0.0:8090-8091->8090-8091/tcp, 0.0.0.0:8190-8191->8190-8191/tcp   hadoop-slave1
c5c094753715        xuzh/hadoop:1.0     "sh -c 'service ssh s"   About a minute ago   Up About a minute   0.0.0.0:8088->8088/tcp, 0.0.0.0:50070->50070/tcp                                             hadoop-master
```

拷贝
```
docker cp /YOUR_PATH/target/app-on-yarn-demo-1.0.0-SNAPSHOT.jar c5c094753715:/root
```

### 4.4 上传JAR包到HDFS
进入`hadoop-master`容器，
```
docker exec -it hadoop-master bash
```

执行，
```
hdfs dfs -rm -f /app-on-yarn-demo-1.0.0-SNAPSHOT.jar && hdfs dfs -put app-on-yarn-demo-1.0.0-SNAPSHOT.jar /
```

### 4.5 运行程序
```
yarn jar app-on-yarn-demo-1.0.0-SNAPSHOT.jar com.neoremind.app.on.yarn.demo.Client \
  -jar_path /root/app-on-yarn-demo-1.0.0-SNAPSHOT.jar \
  -jar_path_in_hdfs hdfs://hadoop-master:9000/app-on-yarn-demo-1.0.0-SNAPSHOT.jar \
  -appname DemoApp \
  -master_memory 1024 \
  -master_vcores 1 \
  -container_memory 1024 \
  -container_vcores 1 \
  -num_containers 3 \
  -memory_overhead 512 \
  -queue default \
  -java_opts "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+UseConcMarkSweepGC" \ 
  -shell_args "abc 123"

```

确认程序运行中。
```
yarn application -list
18/03/22 08:53:19 INFO client.RMProxy: Connecting to ResourceManager at hadoop-master/172.18.0.2:8032
Total number of applications (application-types: [] and states: [SUBMITTED, ACCEPTED, RUNNING]):1
                Application-Id	    Application-Name	    Application-Type	      User	     Queue	             State	       Final-State	       Progress	                       Tracking-URL
application_1521703915580_0003	             DemoApp	                YARN	      root	   default	           RUNNING	         UNDEFINED	             0%	             http://172.18.0.3:8090
```
