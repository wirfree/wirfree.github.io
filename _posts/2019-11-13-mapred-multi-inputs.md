---
layout: post
title:  "Map Reduce的类型与格式 - 多个输入"
date:   2019-11-11 17:12:00 +0800
categories: big data
permalink: /mapred-multi-inputs
---

上一篇文章 **[开发Map Reduce应用](/mapred-app)** 介绍了使用 Maven + Intellij 开发Map Reduce应用，并且打包部署到伪分布式集群。这篇文章会简单举例如何处理输入输出格式，一个最简单的例子就是多重输入，这被应用在需要统计不同格式的数据的场景：他们可能是不同版本的程序生成的，也可能是不同数据集join来的，等等。

## 实现

实现这个主要的步骤是：
1. 创建不同的Mapper去处理不同的数据
2. 使用MultipleInputs.addInputPath为不同的输入绑定相应的Mapper

两个Mapper：

```java
public class MetOfficeMaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private MetOfficeRecordParser parser = new MetOfficeRecordParser();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if (!parser.isValidTemperature()) {
            return;
        }
        String year = parser.getYear();
        int temperature = parser.getAirTemperature();
        context.write(new Text(year), new IntWritable(temperature));
    }
}
```

```java
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String year = line.substring(15, 19);
        int airTemperature;
        if (line.charAt(87) == '+') {
            airTemperature = Integer.parseInt(line.substring(88, 92));
        } else {
            airTemperature = Integer.parseInt(line.substring(87, 92));
        }
        String quality = line.substring(92, 93);
        if (airTemperature != 9999 && quality.matches("[01459]")) {
            context.write(new Text(year), new IntWritable(airTemperature));
        }
    }
}
```

实现Tool.run方法：

```java
...
        Path ncdcInputPath = new Path(args[0]);
        Path metOfficeInputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        MultipleInputs.addInputPath(job, ncdcInputPath,
                TextInputFormat.class, MaxTemperatureMapper.class);
        MultipleInputs.addInputPath(job, metOfficeInputPath,
                TextInputFormat.class, MetOfficeMaxTemperatureMapper.class);
...
```