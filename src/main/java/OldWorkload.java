import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

public class OldWorkload {
    public static void startWorkload() throws IOException, URISyntaxException, InterruptedException {
        final Logger log = LogManager.getLogger(KafkaProducerExample.class);
        Workload wrld = new Workload();
        Random rnd = new Random();
        for (int i = 0; i < wrld.getDatax().size(); i++) {
            log.info("sending a batch of authorizations of size:{}",
                    Math.ceil(wrld.getDatay().get(i)));

            Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
            int p0 = (int) (0.27 * Math.ceil(wrld.getDatay().get(i)));
            int p1 = (int) (0.27 * Math.ceil(wrld.getDatay().get(i)));
            int others = (int) (0.153 * Math.ceil(wrld.getDatay().get(i)));

            log.info("Sending {} to P0 and P1  and {} to each of the others", p0, others);

            for (long j = 0; j < p0; j++) {
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                0, null, UUID.randomUUID().toString(), custm));
            }
            for (long j = 0; j < p1; j++) {
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                1, null, UUID.randomUUID().toString(), custm));
            }
            for (long j = 0; j < others; j++) {
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                2, null, UUID.randomUUID().toString(), custm));
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                3, null, UUID.randomUUID().toString(), custm));
                KafkaProducerExample.
                        producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                                4, null, UUID.randomUUID().toString(), custm));
            }

            Thread.sleep(KafkaProducerExample.config.getDelay());
        }
    }
}
