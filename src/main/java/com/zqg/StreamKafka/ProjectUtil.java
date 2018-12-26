package com.zqg.StreamKafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class ProjectUtil {
	/**
	 * 使用log4j配置打印日志
	 */
	static final Logger logger = Logger.getLogger(KafkaSparkStreamingWithZookeeperoffset.class);
	/**
	 * 加载配置的log4j.properties,默认读取的路径在src下，如果将log4j.properties放在别的路径中要手动加载
	 */
	public static void LoadLogConfig() {
		PropertyConfigurator.configure("D:\\BaiduNetdiskDownload\\03_第三阶段  spark体系之分布式计算【北京尚学堂·百战程序员】\\05_kafka分布式消息队列【北京尚学堂·百战程序员】\\04 - 代码\\SparkStreaming_Kafka_Manage\\resource\\log4j.properties");
	}
	
	/**
	 * 加载配置文件
	 * 需要将放config.properties的目录设置成资源目录
	 * @return
	 */
	public static Properties loadProperties() {

        Properties props = new Properties();
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties");
        if(null != inputStream) {
            try {
                props.load(inputStream);
            } catch (IOException e) {
            	logger.error(String.format("Config.properties file not found in the classpath"));
            }
        }
        return props;

    }
	
	public static void main(String[] args) {
		Properties props = loadProperties();
		String value = props.getProperty("hello");
		System.out.println(value);
	}
}
