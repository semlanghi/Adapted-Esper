package ee.ut.cs.dsg.esperadapter.environment.adapters;

import com.espertech.esper.common.client.EventSender;
import com.espertech.esper.common.internal.collection.Pair;
import com.espertech.esper.runtime.client.EPEventService;
import ee.ut.cs.dsg.esperadapter.util.PerformanceFileBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.function.Function;
import java.util.logging.Logger;

/**
 * An adapter for consuming events from a specific Kafka Topic.
 * Since the input is coming from Kafka, the input events will arrive
 * as key-value pairs. It contains a {@link KafkaConsumer}, set through the {@link Properties}
 * object. Currently, it support homogeneous files, i.e., files that contain
 * events of the same type, sent through the {@link EventSender} object.
 * Every time an event is sent, the time in the {@link com.espertech.esper.runtime.client.EPRuntime}
 * is advanced accordingly, since we are using the external time by default.
 * Optionally, it can register the performance and report it in the performance file.
 *
 * @param <K> The key of the Kafka record
 * @param <V> The value of the Kafka record
 * @param <E> The type of the event sent to Esper
 */

public class KafkaEsperCustomAdapter<K,V,E> implements EsperCustomAdapter<V,E> {

    public static final Logger LOGGER = Logger.getLogger(KafkaEsperCustomAdapter.class.getName());

    private final KafkaConsumer<K,V> consumer;
    private final EventSender sender;
    private final Properties props;
    private final Duration duration;
    private final long maxEvents;
    private long counter = 0;
    private int endCount=0;
    private final boolean registerPerf;

    /**
     * Constructor for normal consumption from a Kafka Topic. It consumes the topic continuously.
     * The number of max events is thus set to -1, and the max duration to zero.
     *
     * @param props Properties for consumer setting
     * @param eventService Event Service for advancing Esper time
     * @param registerPerf Whether we want performance file
     */
    public KafkaEsperCustomAdapter(Properties props, EPEventService eventService, boolean registerPerf) {
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Arrays.asList(props.get(EsperCustomAdapterConfig.TOPIC_NAME).toString().split(",")));
        this.sender = eventService.getEventSender(props.get(EsperCustomAdapterConfig.EVENT_NAME).toString());
        this.props = props;
        this.duration = Duration.ZERO;
        this.maxEvents=-1;
        this.registerPerf=registerPerf;
    }

    /**
     * Constructor for consumption from a Kafka Topic. It consumes the topic continuously until
     * it reaches a specific number of events.
     *
     * @param props Properties for consumer setting
     * @param epEventService Event Service for advancing Esper time
     * @param maxEvents The maximum number of events we want to consume
     * @param registerPerf Whether we want performance file
     */
    public KafkaEsperCustomAdapter(Properties props, EPEventService epEventService, long maxEvents, boolean registerPerf) {
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Arrays.asList(props.get(EsperCustomAdapterConfig.TOPIC_NAME).toString().split(",")));
        this.sender = epEventService.getEventSender(props.get(EsperCustomAdapterConfig.EVENT_NAME).toString());
        this.props = props;
        this.duration = Duration.ZERO;
        this.maxEvents=maxEvents;
        this.registerPerf=registerPerf;
    }

    /**
     * Constructor for consumption from a Kafka Topic. It consumes the topic continuously
     * for a specific amount of time.
     *
     * @param props Properties for consumer setting
     * @param epEventService Event Service for advancing Esper time
     * @param duration The maximum consumption time
     * @param registerPerf Whether we want performance file
     */
    public KafkaEsperCustomAdapter(Properties props, EPEventService epEventService, Duration duration, boolean registerPerf) {
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Arrays.asList(props.get(EsperCustomAdapterConfig.TOPIC_NAME).toString().split(",")));
        this.sender = epEventService.getEventSender(props.get(EsperCustomAdapterConfig.EVENT_NAME).toString());
        this.props = props;
        this.duration = duration;
        this.maxEvents=-1;
        this.registerPerf=registerPerf;
    }

    @Override
    public void process(Function<V, Pair<E,Long>> transformationFunction){
        long startTime = System.currentTimeMillis();
        long endTime = System.currentTimeMillis();
        long diff = endTime - startTime;
        long duration = this.duration.toMillis();

        try{
            /*
            Checks which is the condition tha stop the consumption.
            In case of no stopping criteria, the number of arrived, special ad-hoc "ending events"
            is counted.
             */
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

        if(registerPerf)
            registerPerformance(diff);

    }

    private void registerPerformance(long diff){
        double throughput = (double)counter;
        throughput = throughput/diff;
        throughput = throughput *1000;

        PerformanceFileBuilder performanceFileBuilder = new PerformanceFileBuilder(props.getProperty(EsperCustomAdapterConfig.PERF_FILE_NAME), "ee/ut/cs/dsg/esper", 1);
        performanceFileBuilder.register(props.getProperty(EsperCustomAdapterConfig.STATEMENT_NAME), throughput,
                props.getProperty(EsperCustomAdapterConfig.EXPERIMENT_ID), props.getProperty(EsperCustomAdapterConfig.ON_CLUSTER), counter, diff/1000);
        performanceFileBuilder.close();
    }

    private void send(Pair<E,Long> eventTimestamp){
        /*
        Here we do not advance the timestamp since with multiple partitions it can generate problems
        TODO: create separated timestamp advancements for each context
         */
        sender.sendEvent(eventTimestamp.getFirst());
        counter++;
    }



}
