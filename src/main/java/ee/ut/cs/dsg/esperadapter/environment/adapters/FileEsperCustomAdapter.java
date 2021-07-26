package ee.ut.cs.dsg.esperadapter.environment.adapters;

import com.espertech.esper.common.client.EventSender;
import com.espertech.esper.common.internal.collection.Pair;
import com.espertech.esper.runtime.client.EPEventService;
import ee.ut.cs.dsg.esperadapter.util.PerformanceFileBuilder;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Function;

/**
 * An adapter for consuming events from a specific file.
 * Since the input is coming from a file, the input events will arrive
 * as {@link String}. Currently, it support homogeneous files, i.e., files that contain
 * events of the same type, sent through the {@link EventSender} object.
 * Every time an event is sent, the time in the {@link com.espertech.esper.runtime.client.EPRuntime}
 * is advanced accordingly, since we are using the external time by default.
 * Optionally, it can register the performance and report it in the performance file.
 *
 * @param <E> The type of the event sent to Esper.
 */

public class FileEsperCustomAdapter<E> implements EsperCustomAdapter<String,E> {

    private FileReader fileReader;
    private Properties props;
    private EventSender sender;
    private EPEventService epEventService;
    private boolean registerPerf;
    private long maxEvents;
    private long counter = 0L;

    /**
     * Constructor for normal events generation. It consumes the file until it end.
     * The {@link FileEsperCustomAdapter#maxEvents} variable is thus set to -1.
     *
     * @param props The properties for setting up the eventual consumer
     * @param eventService The event service used to advance time
     * @param registerPerf whether we want to generate the performance file
     */
    public FileEsperCustomAdapter(Properties props, EPEventService eventService, boolean registerPerf) {
        try {
            this.fileReader = new FileReader(props.getProperty(EsperCustomAdapterConfig.INPUT_FILE_NAME));
            this.sender = eventService.getEventSender(props.getProperty(EsperCustomAdapterConfig.EVENT_NAME));
            this.epEventService = eventService;
            this.props=props;
            this.registerPerf=registerPerf;
            this.maxEvents = -1;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Constructor for normal events generation. It consumes the file until it reaches either the end, or
     * a maximum number of events.
     * The {@link FileEsperCustomAdapter#maxEvents} variable is thus set as a parameter.
     *
     * @param props The properties for setting up the eventual consumer
     * @param eventService The event service used to advance time
     * @param registerPerf whether we want to generate the performance file
     * @param maxEvents The maximum number of events we want to consume
     */
    public FileEsperCustomAdapter(Properties props, EPEventService eventService,  boolean registerPerf, long maxEvents) {
        try {
            this.fileReader = new FileReader(props.getProperty(EsperCustomAdapterConfig.INPUT_FILE_NAME));
            this.sender = eventService.getEventSender(props.getProperty(EsperCustomAdapterConfig.EVENT_NAME));
            this.epEventService = eventService;
            this.props=props;
            this.registerPerf=registerPerf;
            this.maxEvents = maxEvents;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void process(Function<String, Pair<E, Long>> transformationFunction) {
        // RegisterIng the starting time
        long startTime = System.currentTimeMillis();

        try (BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            String line;

            // Reading the first line of the file
            bufferedReader.readLine();

            /*
            In case of maxEvents=-1, we consume the whole file.
             */
            if(maxEvents==-1)
                while ((line = bufferedReader.readLine()) != null) {
                    send(transformationFunction.apply(line));
                }
            /*
            Else, we stop when we reach the end of the file, or we reach maximum number of events.
             */
            else while ((line = bufferedReader.readLine()) != null && counter<maxEvents) {
                send(transformationFunction.apply(line));
            }

            // Registering the ending time
            long endTime = System.currentTimeMillis();

            if(registerPerf)
                registerPerformance(endTime - startTime);

            bufferedReader.close();
            fileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    private void registerPerformance(long diff){
        double throughput = (double)counter;
        throughput = throughput/diff;
        throughput = throughput *1000;

        PerformanceFileBuilder performanceFileBuilder = new PerformanceFileBuilder(props.getProperty(EsperCustomAdapterConfig.PERF_FILE_NAME), "esper", 1);
        performanceFileBuilder.register(props.getProperty(EsperCustomAdapterConfig.STATEMENT_NAME), throughput,
                props.getProperty(EsperCustomAdapterConfig.EXPERIMENT_ID), props.getProperty(EsperCustomAdapterConfig.ON_CLUSTER), counter, diff/1000);
        performanceFileBuilder.close();
    }

    private void send(Pair<E,Long> eventTimestamp){
        // Advancing time assuring monotonic advancement
        if(epEventService.getCurrentTime()<eventTimestamp.getSecond())
            epEventService.advanceTime(eventTimestamp.getSecond());

        // Event sending, use sender because more efficient
        sender.sendEvent(eventTimestamp.getFirst());
        counter++;
    }

}
