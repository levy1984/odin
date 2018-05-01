package com.levy.runes.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.Random;
import java.util.UUID;

@Component
@EnableScheduling
public class KafkaProducer {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private KafkaTemplate kafkaTemplate;

    /**
     * 定时任务
     */
    @Scheduled(cron = "0/1 * * * * ?")
    public void send(){
//        String message = UUID.randomUUID().toString();
        Random r = new Random();
        int n1 = r.nextInt(10);
        String message = "cust_"+n1+"-"+n1;
        ListenableFuture future = kafkaTemplate.send("test", message);
//        future.addCallback(o -> System.out.println("send-消息发送成功：" + message), throwable -> System.out.println("消息发送失败：" + message));
        future.addCallback(o -> logger.info("send-消息发送成功：" + message),
                throwable -> logger.info("消息发送失败：" + message));
    }

}
