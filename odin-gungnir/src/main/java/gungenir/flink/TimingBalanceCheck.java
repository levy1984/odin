package gungenir.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;

import java.util.Properties;

@Slf4j
public class TimingBalanceCheck {

    private Properties getKafkaProperties(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.31.13.10:9093,172.31.13.11:9093,172.31.13.12:9093");
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("group.id", "test");
        return properties;
    }

    public void doBanlanceCheck(String topic){
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        checkpoint every 5000 msecs
//        env.enableCheckpointing(5000);

        Properties properties = getKafkaProperties();

        FlinkKafkaConsumer010<ObjectNode> myConsumer = new FlinkKafkaConsumer010(topic, new JSONDeserializationSchema(), properties);

        myConsumer.setStartFromEarliest();     // start from the earliest record possible
        myConsumer.setStartFromLatest();       // start from the latest record
        myConsumer.setStartFromGroupOffsets(); // the default behaviour
        log.info("-----------启动定时余额检查--------------");


        DataStream<ObjectNode> messageStream = env.addSource(myConsumer);

        messageStream.rebalance().map(new MapFunction<ObjectNode, Tuple2<String, Integer>>() {
            private static final long serialVersionUID = -6867736771747690202L;
            @Override
            public Tuple2<String, Integer> map(ObjectNode value) throws Exception {
                return new Tuple2(value.get("acctId").asText(),1);
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print();


        try{
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        TimingBalanceCheck test = new TimingBalanceCheck();
        test.doBanlanceCheck("test");
    }



}
