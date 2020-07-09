package esper;

import com.espertech.esper.common.internal.collection.Pair;
import esper.EsperCustomAdapterConfig;
import esper.KafkaAdaptedEsperEnvironmentBuilder;
import event.SpeedEvent;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.util.Properties;
import java.util.UUID;

public class KafkaAdaptedEsper {

    public static void main(String[] args){

        Properties props = new Properties();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, parameterTool.get("bootstrap", "localhost:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "esper.KafkaAdaptedEsper"+ UUID.randomUUID());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaAdapedEsperClient" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(EsperCustomAdapterConfig.TOPIC_NAME, parameterTool.get("topic", "linear-road-data"));
        props.put(EsperCustomAdapterConfig.EVENT_NAME, "SpeedEvent");
        props.put(EsperCustomAdapterConfig.INPUT_FILE_NAME, "/Users/samuelelanghi/Desktop/linear2.csv");
        File temp = new File("result/results.csv");
        if(!temp.exists())
            temp = new File("scripts/result/results.csv");
        props.put(EsperCustomAdapterConfig.PERF_FILE_NAME, temp.getPath());
        props.put(EsperCustomAdapterConfig.EXPERIMENT_ID, parameterTool.get("exp", "exp1") );
        props.put(EsperCustomAdapterConfig.STATEMENT_NAME, parameterTool.get("query", "Delta"));


        KafkaAdaptedEsperEnvironmentBuilder<Integer,String, SpeedEvent> builder = new KafkaAdaptedEsperEnvironmentBuilder<>(props);



        builder.withBeanType(SpeedEvent.class)
                .addStatement(props.getProperty(EsperCustomAdapterConfig.STATEMENT_NAME))
                .buildRuntime(true)
                .adaptKafka(props, Long.parseLong(parameterTool.get("maxevents", "10000"))).start(s -> {
                    String[] data = s.replace("[","").replace("]","").split(",");
                    long ts = Long.parseLong(data[8].trim());
                    SpeedEvent event = new SpeedEvent(data[0].trim(), ts,Integer.parseInt(data[1].trim()));
                    return new Pair<>(event, ts);
                });

    }
}
