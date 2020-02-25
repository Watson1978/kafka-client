package consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "java-consumer");

    KafkaConsumer<String, String> consumer =
        new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());

    // トピックを設定
    consumer.subscribe(Arrays.asList("test"));

    try {
      while (true) {
        // メッセージ取得
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

        // 表示
        for (ConsumerRecord<String, String> record : records) {
          System.out.println(String.format("%s:%s", record.offset(), record.value()));
        }

        // メッセージの読み取り位置を更新
        consumer.commitSync();
      }
    } finally {
      consumer.close();
    }
  }
}
