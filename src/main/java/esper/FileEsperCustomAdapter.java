package esper;

import com.espertech.esper.common.client.EventSender;
import com.espertech.esper.common.internal.collection.Pair;
import com.espertech.esper.runtime.client.EPEventService;
import esper.util.PerformanceFileBuilder;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Function;

public class FileEsperCustomAdapter<E> implements EsperCustomAdapter<String,E>{

    private FileReader fileReader;
    private Properties props;
    private EventSender sender;
    private EPEventService epEventService;
    private long maxEvents;
    private long counter = 0L;

    public FileEsperCustomAdapter(Properties props, EPEventService eventService) {
        try {
            this.fileReader = new FileReader(props.getProperty(EsperCustomAdapterConfig.INPUT_FILE_NAME));
            this.sender = eventService.getEventSender(props.getProperty(EsperCustomAdapterConfig.EVENT_NAME));
            this.epEventService = eventService;
            this.props=props;
            this.maxEvents = -1;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public FileEsperCustomAdapter(Properties props, EPEventService eventService, long maxEvents) {
        try {
            this.fileReader = new FileReader(props.getProperty(EsperCustomAdapterConfig.INPUT_FILE_NAME));
            this.sender = eventService.getEventSender(props.getProperty(EsperCustomAdapterConfig.EVENT_NAME));
            this.epEventService = eventService;
            this.maxEvents = maxEvents;
            this.props=props;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void process(Function<String, Pair<E, Long>> transformationFunction) {
        long startTime = System.currentTimeMillis();

        try (BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            String line;
            bufferedReader.readLine();
            //first line
            if(maxEvents==-1)
                while ((line = bufferedReader.readLine()) != null) {
                    send(transformationFunction.apply(line));
                }
            else while ((line = bufferedReader.readLine()) != null && counter<maxEvents) {
                send(transformationFunction.apply(line));
            }

            long endTime = System.currentTimeMillis();
            long diff = endTime - startTime;
            double throughput = (double)counter;
            double ddiff = (double) diff;
            throughput = throughput/diff;
            throughput = throughput *1000;

            PerformanceFileBuilder performanceFileBuilder = new PerformanceFileBuilder(props.getProperty(EsperCustomAdapterConfig.PERF_FILE_NAME),"esper", 1);
            performanceFileBuilder.register(props.getProperty(EsperCustomAdapterConfig.STATEMENT_NAME), throughput,
                    props.getProperty(EsperCustomAdapterConfig.EXPERIMENT_ID), false, counter, diff/1000);
            performanceFileBuilder.close();

            bufferedReader.close();
            fileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void send(Pair<E,Long> eventTimestamp){
        if(epEventService.getCurrentTime()<eventTimestamp.getSecond())
            epEventService.advanceTime(eventTimestamp.getSecond());

        sender.sendEvent(eventTimestamp.getFirst());

        counter++;
    }

}
