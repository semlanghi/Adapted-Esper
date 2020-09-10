package ee.ut.cs.dsg.esper.environment;

import com.espertech.esper.common.internal.collection.Pair;
import ee.ut.cs.dsg.esper.d2ia.event.SpeedEvent;
import ee.ut.cs.dsg.esper.environment.adapters.EsperCustomAdapterConfig;
import ee.ut.cs.dsg.esper.util.Util;
import org.apache.flink.api.java.utils.ParameterTool;
import java.util.Properties;


public class MainExperiment {

    public static void main(String[] args){

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Properties props = Util.getConfiguration(parameterTool);

        AdaptedEsperEnvironmentBuilder<Integer,String, SpeedEvent> builder = new AdaptedEsperEnvironmentBuilder<>(props);

        builder.withBeanType(SpeedEvent.class)
                .addStatement(props.getProperty(EsperCustomAdapterConfig.STATEMENT_NAME))
                .buildRuntime(true, parameterTool.has("performance"))
                .fromKafka(props).start(s -> {
                    String[] data = s.replace("[","").replace("]","").split(", ");
                    long ts = Long.parseLong(data[8].trim());
                    SpeedEvent event = new SpeedEvent(data[0].trim(), ts,Integer.parseInt(data[1].trim()));
                    if(Long.parseLong(data[0].trim())==-1){
                       return new Pair<>(event,-1L);
                    }
                    return new Pair<>(event, ts);
                });

    }


}
