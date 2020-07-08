package esper;

import com.espertech.esper.common.client.EventSender;
import com.espertech.esper.common.internal.collection.Pair;
import com.espertech.esper.runtime.client.EPEventService;
import com.google.common.util.concurrent.RateLimiter;
import esper.util.PerformanceFileBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.awt.*;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Function;

public class KafkaEsperCustomAdapter<K,V,E> implements EsperCustomAdapter<V,E>{


    private final KafkaConsumer<K,V> consumer;
    private final EventSender sender;
    private final long maxEvents;
    private long counter = 0;
    private FileWriter writer;
    private Properties props;
    private double eventsPerSecond=-1;
    private ConcurrentLinkedQueue<ConsumerRecord<K,V>> eventQueue;

    public KafkaEsperCustomAdapter(Properties props, EPEventService eventService) {
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Arrays.asList(props.get(EsperCustomAdapterConfig.TOPIC_NAME).toString().split(",")));
        this.sender = eventService.getEventSender(props.get(EsperCustomAdapterConfig.EVENT_NAME).toString());
        this.eventQueue = new ConcurrentLinkedQueue<>();
        this.props = props;
        this.maxEvents=-1;
        try {
            File temp = new File("result/inputs/Kafka-Input-"+props.getProperty(EsperCustomAdapterConfig.STATEMENT_NAME)+"-"+props.getProperty(EsperCustomAdapterConfig.EXPERIMENT_ID)+".csv");
            if(!temp.exists())
                temp = new File("scripts/result/inputs/Kafka-Input-"+props.getProperty(EsperCustomAdapterConfig.STATEMENT_NAME)+"-"+props.getProperty(EsperCustomAdapterConfig.EXPERIMENT_ID)+".csv");
            this.writer = new FileWriter(temp);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public KafkaEsperCustomAdapter(Properties props, EPEventService epEventService, long maxEvents) {
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Arrays.asList(props.get(EsperCustomAdapterConfig.TOPIC_NAME).toString().split(",")));
        this.sender = epEventService.getEventSender(props.get(EsperCustomAdapterConfig.EVENT_NAME).toString());
        this.eventQueue = new ConcurrentLinkedQueue<>();
        this.props = props;
        this.maxEvents=maxEvents;
        try {
            File temp = new File("result/inputs/Kafka-Input-"+props.getProperty(EsperCustomAdapterConfig.STATEMENT_NAME)+"-"+props.getProperty(EsperCustomAdapterConfig.EXPERIMENT_ID)+".csv");
            if(!temp.exists())
                temp = new File("scripts/result/inputs/Kafka-Input-"+props.getProperty(EsperCustomAdapterConfig.STATEMENT_NAME)+"-"+props.getProperty(EsperCustomAdapterConfig.EXPERIMENT_ID)+".csv");
            this.writer = new FileWriter(temp);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public KafkaEsperCustomAdapter(Properties props, EPEventService epEventService, long maxEvents, double eventsPerSecond) {
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Arrays.asList(props.get(EsperCustomAdapterConfig.TOPIC_NAME).toString().split(",")));
        this.sender = epEventService.getEventSender(props.get(EsperCustomAdapterConfig.EVENT_NAME).toString());
        this.eventQueue = new ConcurrentLinkedQueue<>();
        this.eventsPerSecond = eventsPerSecond;
        this.maxEvents=maxEvents;
        try {
            File temp = new File("result/inputs/Kafka-Input-"+props.getProperty(EsperCustomAdapterConfig.STATEMENT_NAME)+"-"+props.getProperty(EsperCustomAdapterConfig.EXPERIMENT_ID)+".csv");
            if(!temp.exists())
                temp = new File("scripts/result/inputs/Kafka-Input-"+props.getProperty(EsperCustomAdapterConfig.STATEMENT_NAME)+"-"+props.getProperty(EsperCustomAdapterConfig.EXPERIMENT_ID)+".csv");
            this.writer = new FileWriter(temp);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(Function<V, Pair<E,Long>> transformationFunction){
        long startTime = System.currentTimeMillis();
        Thread pollingThread = new Thread(new Runnable() {
            @Override
            public void run() {
                RateLimiter limiter = RateLimiter.create(eventsPerSecond);
                while ((maxEvents == -1) || counter < maxEvents) {
                    ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(2));
                    records.forEach(record -> {
                        limiter.acquire();
                        try {
                            writer.write(record.value().toString() + "\n");
                            writer.flush();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        eventQueue.add(record);
                    });
                }
                consumer.close();
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (WakeupException e){
                    // Using wakeup to close consumer
                } finally {
                    consumer.close();
                }
            }
        });

        pollingThread.start();

        while ((maxEvents == -1) || counter < maxEvents) {
            V value;
            try {
                value = eventQueue.poll().value();
                send(transformationFunction.apply(value));
            }catch (NullPointerException e){
            }
        }

        long endTime = System.currentTimeMillis();
        long diff = endTime - startTime;
        double throughput = (double)counter;
        double ddiff = (double) diff;
        throughput = throughput/diff;
        throughput = throughput *1000;

        PerformanceFileBuilder performanceFileBuilder = new PerformanceFileBuilder(props.getProperty(EsperCustomAdapterConfig.PERF_FILE_NAME),"esper", 1);
        performanceFileBuilder.register(props.getProperty(EsperCustomAdapterConfig.STATEMENT_NAME), throughput,
                props.getProperty(EsperCustomAdapterConfig.EXPERIMENT_ID), false, counter);
        performanceFileBuilder.close();

    }

    private void send(Pair<E,Long> eventTimestamp){
//        if(epEventService.getCurrentTime()<eventTimestamp.getSecond())
//            epEventService.advanceTime(eventTimestamp.getSecond());
        sender.sendEvent(eventTimestamp.getFirst());
        counter++;
        System.out.println(counter);
    }



}
