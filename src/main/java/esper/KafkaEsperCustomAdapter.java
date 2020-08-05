package esper;

import com.espertech.esper.common.client.EventSender;
import com.espertech.esper.common.internal.collection.Pair;
import com.espertech.esper.runtime.client.EPEventService;
import esper.util.PerformanceFileBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Function;

public class KafkaEsperCustomAdapter<K,V,E> implements EsperCustomAdapter<V,E>{


    private final KafkaConsumer<K,V> consumer;
    private final EventSender sender;
    private final long maxEvents;
    private long counter = 0;
    private Properties props;
    private final Duration duration;
    private int endCount=0;

    public KafkaEsperCustomAdapter(Properties props, EPEventService eventService) {
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Arrays.asList(props.get(EsperCustomAdapterConfig.TOPIC_NAME).toString().split(",")));
        this.sender = eventService.getEventSender(props.get(EsperCustomAdapterConfig.EVENT_NAME).toString());
        this.props = props;
        this.maxEvents=-1;
        this.duration = Duration.ZERO;
    }


    public KafkaEsperCustomAdapter(Properties props, EPEventService epEventService, long maxEvents) {
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Arrays.asList(props.get(EsperCustomAdapterConfig.TOPIC_NAME).toString().split(",")));
        this.sender = epEventService.getEventSender(props.get(EsperCustomAdapterConfig.EVENT_NAME).toString());
        this.props = props;
        this.maxEvents=maxEvents;
        this.duration = Duration.ZERO;
    }

    public KafkaEsperCustomAdapter(Properties props, EPEventService epEventService, Duration duration) {
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Arrays.asList(props.get(EsperCustomAdapterConfig.TOPIC_NAME).toString().split(",")));
        this.sender = epEventService.getEventSender(props.get(EsperCustomAdapterConfig.EVENT_NAME).toString());
        this.props = props;
        this.maxEvents=-1;
        this.duration = duration;
    }

    public KafkaEsperCustomAdapter(Properties props, EPEventService epEventService, long maxEvents, double eventsPerSecond) {
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Arrays.asList(props.get(EsperCustomAdapterConfig.TOPIC_NAME).toString().split(",")));
        this.sender = epEventService.getEventSender(props.get(EsperCustomAdapterConfig.EVENT_NAME).toString());
        this.maxEvents=maxEvents;
        this.duration = Duration.ZERO;
    }

    @Override
    public void process(Function<V, Pair<E,Long>> transformationFunction){
        long startTime = System.currentTimeMillis();
        long endTime = System.currentTimeMillis();
        long diff = endTime - startTime;
        long duration = this.duration.toMillis();

        try{
            while (((maxEvents == -1) || counter < maxEvents) && ((this.duration.isZero()) || duration>diff) && endCount<9) {

                ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(2));
                records.forEach(record -> {
                    Pair<E, Long> value = transformationFunction.apply(record.value());
                    if (value.getSecond() == -1) {
                        endCount++;
                    }else{
                        send(value);
                    }
                });

                endTime = System.currentTimeMillis();
                diff = endTime - startTime;
            }
        } catch (WakeupException e) {
            // Using wakeup to close consumer
        } finally {
            consumer.close();
        }

        double throughput = (double)counter;
        throughput = throughput/diff;
        throughput = throughput *1000;

        PerformanceFileBuilder performanceFileBuilder = new PerformanceFileBuilder(props.getProperty(EsperCustomAdapterConfig.PERF_FILE_NAME),"esper", 1);
        performanceFileBuilder.register(props.getProperty(EsperCustomAdapterConfig.STATEMENT_NAME), throughput,
                props.getProperty(EsperCustomAdapterConfig.EXPERIMENT_ID), false, counter, diff/1000);
        performanceFileBuilder.close();

    }

    private void send(Pair<E,Long> eventTimestamp){
        sender.sendEvent(eventTimestamp.getFirst());
        counter++;
    }



}
