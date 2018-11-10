/**   
* @Title: DataxKafkaWriter.java 
* @Package com.ytkj.dataxkafka.datax_kafka.write 
* @Description: TODO(用一句话描述该文件做什么) 
* @author 123774135@qq.com
* @date 2018年11月9日 下午1:41:56 
* @version V1.0   
*/

package com.ytkj.dataxkafka.datax_kafka.write;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;



/** 
* @ClassName: DataxKafkaWriter 
* @Description: datax 扩展kafka 
* @author 123774135@qq.com
* @date 2018年11月9日 下午1:41:56 
*  
*/

public class DataxKafkaWriter extends Writer{

	public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originConfig = null;
        @Override
        public void init() {
            this.originConfig = this.getPluginJobConf();
           
        }

        @Override
        public void  prepare(){
            super.prepare();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> splitResultConfigs = new ArrayList<Configuration>();
            for (int j = 0; j < mandatoryNumber; j++) {
                splitResultConfigs.add(originConfig.clone());
            }
            return splitResultConfigs;
        }

        
        
        @Override
        public void destroy() {
        	super.prepare();
        }
    }
	
	public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration taskConfig;
        private String bootstrapServers;
        private String produceracks;
        private String kafka_producer_retries;
        private String batchsize;
        private String lingerms;
        private String buffermemory;
        private String topic;
        private String key;
        private String attrue;
        Properties properties = new Properties();
        private String [] cols_attr = null;
        private KafkaProducer<String,String> kafkaProducer;
        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            this.bootstrapServers = this.taskConfig.getNecessaryValue(Key.KAFKA_PRODUCER_BOOTSTRAP_SERVERS, KafkaErrorCode.BAD_CONFIG_VALUE);
            this.batchsize = this.taskConfig.getNecessaryValue(Key.KAFKA_PRODUCER_BATCH_SIZE, KafkaErrorCode.BAD_CONFIG_VALUE);
            this.lingerms = this.taskConfig.getNecessaryValue(Key.KAFKA_PRODUCER_LINGER_MS, KafkaErrorCode.BAD_CONFIG_VALUE);
            this.produceracks = this.taskConfig.getNecessaryValue(Key.KAFKA_PRODUCER_ACKS, KafkaErrorCode.BAD_CONFIG_VALUE);
            this.buffermemory = this.taskConfig.getNecessaryValue(Key.KAFKA_PRODUCER_BUFFER_MEMORY, KafkaErrorCode.BAD_CONFIG_VALUE);
            this.kafka_producer_retries = this.taskConfig.getNecessaryValue(Key.KAFKA_PRODUCER_RETRIES, KafkaErrorCode.BAD_CONFIG_VALUE);
            this.topic = this.taskConfig.getNecessaryValue(Key.KAFKA_TOPIC, KafkaErrorCode.BAD_CONFIG_VALUE);
            this.key = this.taskConfig.getNecessaryValue(Key.KAFKA_KEY, KafkaErrorCode.BAD_CONFIG_VALUE);
            this.attrue = this.taskConfig.getNecessaryValue(Key.ATTRUE_KEY, KafkaErrorCode.BAD_CONFIG_VALUE);
            properties.put("bootstrap.servers", this.bootstrapServers);
            properties.put("acks", this.produceracks);
            properties.put("retries", this.kafka_producer_retries);
            properties.put("batch.size", this.batchsize);
            properties.put("linger.ms", this.lingerms);
            properties.put("buffer.memory", this.buffermemory);
            properties.put("key.serializer", Key.KAFKA_PRODUCER_KEY_SERIALIZER);
            properties.put("value.serializer", Key.KAFKA_PRODUCER_VALUE_SERIALIZER);
            kafkaProducer = new KafkaProducer<String,String>(properties); 
            cols_attr = attrue.split(",");
        }

        


        @Override
        public void destroy() {
        	kafkaProducer.close(6, TimeUnit.SECONDS);
        }

		@Override
		public void startWrite(RecordReceiver recordReceiver) {
			 Record record = null;
			 long totla = 0;
	         ProducerRecord<String,String> producerRecord = null;
             while ((record = recordReceiver.getFromReader()) != null) {
            	// System.out.println("获得的数据为:"+record.toString());//这里的record 根据不同的数据库需要进行加工
            	 //{"data":[{"byteSize":5,"rawData":"11111","type":"STRING"},{"byteSize":5,"rawData":"11111","type":"STRING"},{"byteSize":5,"rawData":"11111","type":"STRING"},{"byteSize":4,"rawData":"1111","type":"STRING"},{"byteSize":6,"rawData":"111111","type":"STRING"}],"size":5}
            	 int recordLength =  record.getColumnNumber();
            	 JSONObject json = new JSONObject(16);
            	 if (0 != recordLength) {
                     Column column;
                     for (int i = 0; i < recordLength; i++) {
                         column = record.getColumn(i);
                         String attre = cols_attr[i];
                         if (null != column.getRawData()) {
                        	// System.out.println("-------------"+column.asString());
                        	 json.put(attre, column.asString()) ;  
                         } else {
                             // warn: it's all ok if nullFormat is null
                        	 json.put(attre, "") ;  
                         }
                     }
                 }
            	 
            	 //System.out.println(" 最后的字符串+"+json.toJSONString());
            	 String json_str = json.toJSONString();
//                 new TextCsvWriterManager().produceUnstructuredWriter(fileFormat, fieldDelimiter, writer).writeOneRecord(splitedRows);
            	String key_word = StringUtils.defaultString(this.key, "datax_kafka_product");
            	producerRecord = new ProducerRecord<String, String>(topic,key_word, json_str);
            	kafkaProducer.send(producerRecord, new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if(exception != null) {
							exception.printStackTrace();
						 } else {
							 if(LOG.isDebugEnabled()) {
								 LOG.debug("The offset of the record we just sent is: " + metadata.offset()); 
							 }
						         
						  }
					}
            	 });
            	 totla = totla + 1;
            }
             
             String msg = String.format("task end, write size :%d", totla);
             getTaskPluginCollector().collectMessage("writesize", String.valueOf(totla));
             LOG.info(msg);
			
		}
    }
	
	public enum KafkaErrorCode implements ErrorCode{
		
		BAD_CONFIG_VALUE("ESWriter-00", "您配置的值不合法.");

	    private final String code;
	    private final String description;

	    KafkaErrorCode(String code, String description) {
	        this.code = code;
	        this.description = description;
	    }

	    @Override
	    public String getCode() {
	        return this.code;
	    }

	    @Override
	    public String getDescription() {
	        return this.description;
	    }

	    @Override
	    public String toString() {
	        return String.format("Code:[%s], Description:[%s]. ", this.code,
	                this.description);
	    }
		
	}
}
