package ee.ut.cs.dsg.esperadapter.environment;

import com.espertech.esper.common.internal.collection.Pair;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPUndeployException;
import ee.ut.cs.dsg.esperadapter.environment.adapters.EsperCustomAdapter;
import ee.ut.cs.dsg.esperadapter.environment.adapters.FileEsperCustomAdapter;
import ee.ut.cs.dsg.esperadapter.environment.adapters.KafkaEsperCustomAdapter;

import java.time.Duration;
import java.util.Properties;
import java.util.function.Function;
import java.util.logging.Logger;

/**
 * Esper Environment that contains the actual {@link EPRuntime} and the {@link EsperCustomAdapter}.
 * It provides the actual DSL for the creation of the Esper instance.
 *
 * @param <V> The type of the event coming out of the source
 * @param <E> The type of the event sent to Esper
 */
public class AdaptedEsperEnvironment<V,E> {

    private final EPRuntime runtime;
    private final Logger LOGGER = Logger.getLogger(AdaptedEsperEnvironment.class.getName());
    private EsperCustomAdapter<V,E> adapter;
    private boolean registerPerf;

    public AdaptedEsperEnvironment(EPRuntime runtime) {
        this.runtime = runtime;
        this.registerPerf=false;
        this.adapter= transformationFunction -> LOGGER.warning("Empty Source");
    }

    public AdaptedEsperEnvironment(EPRuntime runtime, boolean registerPerf) {
        this.runtime = runtime;
        this.registerPerf=registerPerf;
        this.adapter= transformationFunction -> LOGGER.warning("Empty Source");
    }

    private AdaptedEsperEnvironment(EPRuntime runtime, EsperCustomAdapter<V, E> adapter) {
        this.runtime = runtime;
    }

    public AdaptedEsperEnvironment<V,E> fromKafka(Properties props){
        this.adapter = new KafkaEsperCustomAdapter<>(props, runtime.getEventService(), registerPerf);
        return this;
    }

    public AdaptedEsperEnvironment<V,E> fromKafka(Properties props, long maxEvents){
        this.adapter = new KafkaEsperCustomAdapter<>(props, runtime.getEventService(), maxEvents, registerPerf);
        return this;
    }

    public AdaptedEsperEnvironment<V,E> fromKafka(Properties props, Duration minutes){
        this.adapter = new KafkaEsperCustomAdapter<>(props, runtime.getEventService(), minutes, registerPerf);
        return this;
    }

    public AdaptedEsperEnvironment<String,E> fromFile(Properties props){
        return new AdaptedEsperEnvironment<>(this.runtime, new FileEsperCustomAdapter<E>(props, runtime.getEventService(), registerPerf));
    }

    public AdaptedEsperEnvironment<String,E> fromFile(Properties props, long maxEvents){
        return new AdaptedEsperEnvironment<>(this.runtime, new FileEsperCustomAdapter<E>(props, runtime.getEventService(), registerPerf, maxEvents));
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
