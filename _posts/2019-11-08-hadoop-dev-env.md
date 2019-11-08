---
layout: post
title:  "准备hadoop的开发环境"
date:   2019-11-08 16:15:00 +0800
categories: big data
---

文章主要介绍在Windows配置Hadoop的开发环境，一般我们的开发环境都是使用Windows或者MacBook，大家比较多的都使用Windows，毕竟MacBook好贵，一般咱们有iOS开发需求的才去买一台吧~

---

## 工具

- hadoop 3.1.0 + apache-hadoop-3.1.0-winutils [下载链接](/downloads/hadoop-3.1.0.zip)
- [Java 1.8 JDK](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
- [Intellij](https://www.jetbrains.com/idea/download/#section=windows)

## 配置步骤
首先安装JDK，确认环境变量和Path是否正确配置：

![image](/downloads/java_home_env.jpg)

![image](/downloads/java_path.jpg)

这里值得注意的是hadoop不能认带空格的路径，所以JDK安装时自动配置的JAVA_HOME环境变量可能需要改该。如果JDK是安装在C:\Program Files或者是C:\Program Files (x86)下面，那你可以用下面的地址去替换：

原来的写法 | 新的写法
--- | ---
C:\Program Files | C:\Progra~1
C:\Program Files (x86) | C:\Progra~2

将hadoop的压缩包解压到文件中

![image](/downloads/hadoop_dir.jpg)

然后配置环境变量和Path

![image](/downloads/hadoop_env.jpg)

![image](/downloads/hadoop_path.jpg)

现在，我们验证一下hadoop有没有正确安装，打开windows命令行，输入：

```
hadoop version
```

![image](/downloads/hadoop_version.jpg)

如果没有抛出错误并且成功显示hadoop的版本，那就说明hadoop已经成功安装在你的系统中了

## 在Intellij编写hadoop job，并执行和调试

首先一点要注意的是Intellij必须用**管理员权限**启动，不然的话在执行job的过程中会抛出io异常

打开Intellij，创建一个maven工程：

![image](/downloads/create_maven.jpg)

点击Next，创建一个工程叫WordCount

![image](/downloads/word_count.jpg)

点击Next

![image](/downloads/word_count2.jpg)

点击Finish

然后建好的工程我们给它加上hadoop的依赖

右键工程名，点击Open Module Settings，选择dependencies Tab，点击右边的**+**号，选择Jars Or Directories, 然后添加下面选中的库

![image](/downloads/hadoop_dep1.jpg)

![image](/downloads/hadoop_dep2.jpg)

在WordCount/src/main/java下面添加WordCount类：

```java
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
    public static class MyMapper extends
            Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, new IntWritable(1));
            }

        }
    }
    public static class MyReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean status = job.waitForCompletion(true);
        if (status) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}
```

然后做Run Configuration，并且加入一个Application: (Run->Edit Configurations)

![image](/downloads/word_count_setting.jpg)

看上面的运行配置，我们有个input文件夹，在它下面是要分析的文件，我们建两个文件**file0**和**file1**

file0:

```
Hello World Bye World
```

file1:

```
Hello Hadoop Goodbye Hadoop
```

现在，我们运行WordCount，可以看到，在output文件夹中的运行结果：

![image](/downloads/output.jpg)

现在，你可以debug你的hadoop应用，step into到hadoop的源码去看里面到底是怎么玩儿的。从现在开始，现在你可以开发你更高级的hadoop应用了。