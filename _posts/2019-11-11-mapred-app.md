---
layout: post
title:  "开发Map Reduce应用"
date:   2019-11-11 17:12:00 +0800
categories: big data
permalink: /mapred-app
---
## 回顾

上一篇文章 **[准备hadoop的开发环境](/hadoop-dev-env)** 介绍了如何在Windows上搭建Hadoop 3.1.0本地模式的部署，并且介绍了如何在Intellij上面开发第一个Map Reduce程序。

这篇文章会介绍使用Maven创建一个完成的Map Reduce应用，包括在pom.xml中管理hadoop的依赖，应用打包。另外介绍搭建Hadoop的伪分布式部署，并且如何使用不同的配置将应用分环境部署。

## 新建Map Reduce应用

我们参照Hadoop权威指南第六章的例子 [ch06-mr-dev](https://github.com/tomwhite/hadoop-book/tree/master/ch06-mr-dev) 先创建一个pom.xml文件

```
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>club.rongbao</groupId>
    <artifactId>hadoop-book-mr-dev</artifactId>
    <version>4.0</version>
    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <hadoop.version>3.1.0</hadoop.version>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>${hadoop.version}</version>
        </dependency>
    </dependencies>
    <build>
        <finalName>hadoop-examples</finalName>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.1</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-jar-plugin</artifactId>
                <version>2.5</version>
                <configuration>
                    <outputDirectory>${basedir}</outputDirectory>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

注意我们只依赖了hadoop-client，它聚合了所有我们需要的依赖，参照它的[pom.xml](https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client/3.1.0/hadoop-client-3.1.0.pom)文件，包括了hadoop-common，hadoop-hdfs-client等依赖。我们不打包依赖到lib包，一方面是因为没有第三方依赖:D，另一方面说，job和lib可以分开打包，这样依赖不需要在job的jar包里面重新创建，而且把依赖添加到分布式缓存在集群上有更少的jar文件转移。

最后在src/main/java中依次建这四个java文件：

一. NcdcRecordParser - 翻译每行数据，抽取各个字段

```java
import org.apache.hadoop.io.Text;

public class NcdcRecordParser {
    private static final int MISSING_TEMPERATURE = 9999;

    private String year;
    private int airTemperature;
    private boolean airTemperatureMalformed;
    private String quality;

    public void parse(String record) {
        year = record.substring(15, 19);
        airTemperatureMalformed = false;
        // Remove leading plus sign as parseInt doesn't like them (pre-Java 7)
        if (record.charAt(87) == '+') {
            airTemperature = Integer.parseInt(record.substring(88, 92));
        } else if (record.charAt(87) == '-') {
            airTemperature = Integer.parseInt(record.substring(87, 92));
        } else {
            airTemperatureMalformed = true;
        }
        quality = record.substring(92, 93);
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public boolean isValidTemperature() {
        return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE
                && quality.matches("[01459]");
    }

    public boolean isMalformedTemperature() {
        return airTemperatureMalformed;
    }

    public String getYear() {
        return year;
    }

    public int getAirTemperature() {
        return airTemperature;
    }
}
```

二. MaxTemperatureMapper - Mapper，生成year-temperature的键值对

```java
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    enum Temperature {
        MALFORMED
    }

    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        parser.parse(value);
        if (parser.isValidTemperature()) {
            int airTemperature = parser.getAirTemperature();
            context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
        } else if (parser.isMalformedTemperature()) {
            System.err.println("Ignoring possibly corrupt input: " + value);
            context.getCounter(Temperature.MALFORMED).increment(1);
        }
    }
}
```

三. MaxTemperatureReducer - Reducer，负责统计每年最高气温

```java
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTemperatureReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        int maxValue = Integer.MIN_VALUE;
        for (IntWritable value : values) {
            maxValue = Math.max(maxValue, value.get());
        }
        context.write(key, new IntWritable(maxValue));
    }
}
```

四. MaxTemperatureDriver 
- 程序入口，通过Tool接口支持-conf选项用来做配置管理，可以覆盖默认配置
- 指定Mapper, Reducer, Combiner, 注意Combiner和Reducer是同一个类，它是用来在节点中先做好部分组合的工作以减少带宽的消耗。

```java
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTemperatureDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = new Job(getConf(), "Max temperature");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
        System.exit(exitCode);
    }
}
```

最后配置Run Configuration，程序参数为input output

![image](/downloads/hadoopbookmrdev_config.jpg)

注意input是输入文件夹，里面加个sample.txt文件 ([链接](https://github.com/tomwhite/hadoop-book/blob/master/ch06-mr-dev/input/ncdc/micro/sample.txt))。在Intellij中运行它，就相当于跑一个本地部署的job。

## 配置伪分布式部署

#### 配置ssh

在gitbash里面执行下面命令：<br>
```
  $ ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa #如果原来已经有没有passphase的sshkey，跳过这步
  $ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
  $ chmod 0600 ~/.ssh/authorized_keys
```
启动ssh服务：<br>
`net start sshd`

关于ssh服务的安装，配置，和启动也可以参照这篇文章 ([链接](https://stayrelevant.globant.com/en/installing-hadoop-on-windows-linux-mac/))

#### 配置hdfs：

etc/hadoop/core-site.xml:<br>
```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
```
etc/hadoop/hdfs-site.xml:<br>
```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
```

#### 启动hdfs并在本地执行job

假设%HADOOP_HOME%/bin已经在你的path中了，那么你可以执行下面的命令：

1. 格式化hdfs文件系统<br>
`hdfs namenode -format`
2. 启动NameNode和DataNode守护进程<br>
`%HADOOP_HOME%\sbin\start-dfs.cmd`
3. 启动NameNode的web界面
  - NameNode - http://localhost:9870/
4. 创建输入文件夹<br>
`hdfs dfs -mkdir -p input`
5. 拷贝文件到分布式文件系统<br>
`hdfs dfs -put %HADOOP_HOME%\etc\hadoop\*.xml input`
6. 运行安装包给的例子<br>
`hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.1.0.jar grep input output 'dfs[a-z.]+'`
7. 从分布式文件系统中捞文件到本地文件系统<br>
  上一步输出文件到分布式文件系统的output文件夹中，现在我们要把它拷贝到本地文件系统中<br>
  `hdfs dfs -get output output`
8. 关闭守护进程
  `%HADOOP_HOME%\sbin\stop-dfs.cmd`

#### 配置YARN

配置环境变量HADOOP_MAPRED_HOME<br><br>
  ![image](/downloads/mapred_env.jpg)

etc/hadoop/mapred-site.xml:<br>
```xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.application.classpath</name>
    <value>%HADOOP_MAPRED_HOME%/share/hadoop/mapreduce/*,%HADOOP_MAPRED_HOME%/share/hadoop/mapreduce/lib/*</value>
  </property>
</configuration>
```

etc/hadoop/yarn-site.xml:<br>
```xml
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
    </property>
</configuration>
```

#### 跑起来

运行hdfs和yarn：

`%HADOOP_HOME%\sbin\start-all.cmd`

跑一开始的时候的那个Map Reduce应用：

1. `mvn clean package`
2. 删除input和output目录，拷贝输入文件到input<br>
  `hdfs dfs -rm -r -f output`
  `hdfs dfs -rm -r -f input`
  `hdfs dfs -put -p /local/path/to/input/sample.txt input`
3. cd到工程目录，然后执行<br>
  `hadoop jar hadoop-examples.jar MaxTemperatureDriver input output`
4. 捞出来output
  `hadoop fs -getmerge output max-temp`
  最后在本地文件系统拿到合并好的文件max-temp