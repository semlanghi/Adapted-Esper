package esper;

import com.espertech.esper.common.internal.collection.Pair;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPUndeployException;

import java.time.Duration;
import java.util.Properties;
import java.util.function.Function;

public class EsperEnvironment<K,V,E> {

    private EPRuntime runtime;
    private EsperCustomAdapter<V,E> adapter;

    public EsperEnvironment(EPRuntime runtime) {
        this.runtime = runtime;
    }

    public EsperEnvironment<K,V,E> adaptKafka(Properties props){
        this.adapter = new KafkaEsperCustomAdapter<>(props, runtime.getEventService());
        return this;
    }

    public EsperEnvironment(EPRuntime runtime, EsperCustomAdapter<V, E> adapter) {
        this.runtime = runtime;
        this.adapter = adapter;
    }

    public EsperEnvironment<K,V,E> adaptKafka(Properties props, long maxEvents){
        this.adapter = new KafkaEsperCustomAdapter<>(props, runtime.getEventService(), maxEvents);
        return this;
    }

    public EsperEnvironment<K,V,E> adaptKafka(Properties props, long maxEvents, double eventsPerSecond){
        this.adapter = new KafkaEsperCustomAdapter<>(props, runtime.getEventService(), maxEvents, eventsPerSecond);
        return this;
    }

    public EsperEnvironment<K,V,E> adaptKafka(Properties props, Duration minutes){
        this.adapter = new KafkaEsperCustomAdapter<>(props, runtime.getEventService(), minutes);
        return this;
    }

    public EsperEnvironment<?,String,E> adaptFile(Properties props){
        return new EsperEnvironment<>(this.runtime, new FileEsperCustomAdapter<E>(props, runtime.getEventService()));
    }

    public EsperEnvironment<?,String,E> adaptFile(Properties props, long maxEvents){
        return new EsperEnvironment<>(this.runtime, new FileEsperCustomAdapter<E>(props, runtime.getEventService(), maxEvents));
    }


    public void start(Function<V, Pair<E,Long>> transformationFunction){
        adapter.process(transformationFunction);
        try {
            runtime.getDeploymentService().undeployAll();
        } catch (EPUndeployException e) {
            e.printStackTrace();
        }
    }



}
