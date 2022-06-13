import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static java.time.Instant.now;

public class KafkaProducerExample {
    private static final Logger log = LogManager.getLogger(KafkaProducerExample.class);
    static KafkaProducerConfig config;
    static KafkaProducer<String, Customer> producer;
    static Random rnd;

    static int eventsPerSeconds;

    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        rnd = new Random();
        config = KafkaProducerConfig.fromEnv();
        log.info(KafkaProducerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaProducerConfig.createProperties(config);
        int delay = config.getDelay();
        producer = new KafkaProducer<String, Customer>(props);
        log.info("Sending {} messages ...", config.getMessageCount());




/*
        fifteenEpsToeachPartitonForTwoMinutes();
        P1P260EPSOthers15EPSForTwoMinutes();
        P1P2P360EPSOthers15EPSForTwoMinutes();*/


       // ModifiedWorkload md = new ModifiedWorkload();

        OldWorkload.startWorkload();

    }

}

