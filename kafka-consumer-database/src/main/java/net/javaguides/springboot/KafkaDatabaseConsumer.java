package net.javaguides.springboot;


import net.javaguides.springboot.entity.WikimediaData;
import net.javaguides.springboot.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;



@Component
public class KafkaDatabaseConsumer {

    private static  final Logger LOGGER = LoggerFactory.getLogger(KafkaDatabaseConsumer.class);

    @Autowired
    private WikimediaDataRepository dataRepository;
    @KafkaListener(
            topics = "${spring.kafka.topic.name}",
            groupId = "${spring.kafka.consumer.group-id}"
    )


    public void consume(String eventMessage) {
        LOGGER.info(String.format("Event message received -> %s", eventMessage));

        if (eventMessage != null) {
            // Truncate the eventMessage if it's too long
            int maxLength = 250;
            String truncatedEventMessage = eventMessage.substring(0, Math.min(eventMessage.length(), maxLength));

            WikimediaData wikimediaData = new WikimediaData();
            wikimediaData.setWikiEventData(truncatedEventMessage);
            dataRepository.save(wikimediaData);
        } else {
            // Handle the case where eventMessage is null, e.g., log an error or take appropriate action.
            LOGGER.error("Received null event message.");
        }
    }

}



