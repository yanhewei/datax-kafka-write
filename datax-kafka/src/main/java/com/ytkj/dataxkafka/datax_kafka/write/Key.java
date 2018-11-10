package com.ytkj.dataxkafka.datax_kafka.write;

/**
 * Hello world!
 *
 */
public final class Key 
{
    protected static final String KAFKA_PRODUCER_ACKS = "ack";
    
    //消息发送最大尝试次数
    protected static final String KAFKA_PRODUCER_RETRIES = "retries";
    //一批消息处理大小
    protected static final String KAFKA_PRODUCER_BATCH_SIZE = "batchsize";
    //#请求延时
    protected static final String KAFKA_PRODUCER_LINGER_MS = "lingerms";
    //#发送缓存区内存大小
    protected static final String KAFKA_PRODUCER_BUFFER_MEMORY ="buffermemory";
   
    //kafka的brokerservers
    protected static final String KAFKA_PRODUCER_BOOTSTRAP_SERVERS="bootstrapservers";
    
    //#key序列化
    protected static final String KAFKA_PRODUCER_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    //#value序列化
    protected static final String KAFKA_PRODUCER_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    
    //topic
    
    protected static final String KAFKA_TOPIC = "topic";
    
    protected static final String KAFKA_KEY = "key";
    
    
    protected static final String ATTRUE_KEY = "attrue";
}
