import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.w3c.dom.ls.LSOutput;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerApp {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("boorstrap.servers", "BROKER-1:9092, BROKER-2:9093");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // El mensaje tiene que coincidir
        properties.put("group.id", "test");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);

        ArrayList<String> topics = new ArrayList<>();   // Para los topicos
        topics.add("my_topic");
        topics.add("my_topic2"); // Si quieres añadir otro
        kafkaConsumer.subscribe(topics);

        kafkaConsumer.unsubscribe();   // Para dejar todos los topics

        TopicPartition partition0 = new TopicPartition("my_topic", 0); // Para las particiones
        TopicPartition partitionOther2 = new TopicPartition("my_topic2", 2);
        ArrayList<TopicPartition> partitions = new ArrayList<>();
        partitions.add(partition0);
        partitions.add(partitionOther2);
        kafkaConsumer.assign(partitions);

       try{
           while (true){
               ConsumerRecords<String, String> records = kafkaConsumer.poll(100);

               // Logica de procesamiento de los mensajes va aqui
                    // Ejemplo
               for (ConsumerRecord<String, String> record : records){
                   System.out.println(
                           String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s",
                                   record.topic(), record.partition(), record.offset(), record.key(), record.value())
                   );
               }
           }
       }catch (Exception e){
           System.out.println(e.getMessage());
       }finally {
           kafkaConsumer.close();
       }

    }
}
