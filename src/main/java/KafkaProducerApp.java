import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerApp {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("boorstrap.servers", "BROKER-1:9092, BROKER-2:9093");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // El mensaje tiene que coincidir

        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        ProducerRecord producerRecord = new ProducerRecord<>("my_topic", "My Message 1");  // El mensaje coincide
        ProducerRecord producerRecord2 = new ProducerRecord<>("my_topic",2, 124535L,
                "Course-001", "My Message 1"); // Con mas espeficicaiones

        try {
            kafkaProducer.send(producerRecord);
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            kafkaProducer.close();
        }

    }

}
