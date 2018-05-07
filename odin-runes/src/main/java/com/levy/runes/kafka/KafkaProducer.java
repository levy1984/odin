package com.levy.runes.kafka;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.Random;

@Component
@EnableScheduling
@Slf4j
public class KafkaProducer {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    /**
     * 定时任务
     */
    @Scheduled(cron = "0/1 * * * * ?")
    public void send(){
        Random r = new Random();
        int n1 = r.nextInt(10);
        AccountInfo acctInfo = new AccountInfo();
        acctInfo.setCustId("666600000001");
        acctInfo.setAcctId(String.valueOf(n1));
        ListenableFuture future = kafkaTemplate.send("test", JSON.toJSONString(acctInfo));
        future.addCallback(o -> log.info("send-消息发送成功：" + acctInfo), throwable -> log.info("消息发送失败：" + acctInfo));
    }

}
