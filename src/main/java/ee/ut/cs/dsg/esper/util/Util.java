package ee.ut.cs.dsg.esper.util;

import ee.ut.cs.dsg.esper.environment.adapters.EsperCustomAdapterConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.UUID;

public class Util {

    public static Properties getConfiguration(ParameterTool parameterTool){
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, parameterTool.get("bootstrap", "localhost:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ee.ut.cs.esper.environment.KafkaAdaptedEsper"+ UUID.randomUUID());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaAdapedEsperClient" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(EsperCustomAdapterConfig.TOPIC_NAME, parameterTool.get("topic", "linear-road-data"));
        props.put(EsperCustomAdapterConfig.EVENT_NAME, "SpeedEvent");
        props.put(EsperCustomAdapterConfig.INPUT_FILE_NAME, parameterTool.get("file"));
        props.put(EsperCustomAdapterConfig.PERF_FILE_NAME, parameterTool.get("performance"));
        props.put(EsperCustomAdapterConfig.EXPERIMENT_ID, parameterTool.get("exp", "exp") );
        props.put(EsperCustomAdapterConfig.STATEMENT_NAME, parameterTool.get("query", "Aggregate"));
        props.put(EsperCustomAdapterConfig.ON_CLUSTER, parameterTool.get("cluster", "false"));

        return props;
    }
}
