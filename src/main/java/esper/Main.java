package esper;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import event.SpeedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;


public class Main {

    public static void main(String[] args) throws InterruptedException {


        EPCompiler compiler = EPCompilerProvider.getCompiler();


        Configuration configuration = new Configuration();
        configuration.getCommon().addEventType(SpeedEvent.class);

        String epl1 = "@name('my-statement') select * from SpeedEvent\n" +
                "match_recognize (\n" +
                "  partition by key" +
                "  measures max(A.value) as value, count(A.value) as num, first(A.key) as key\n" +
                "  after match skip to current row " +
                "  pattern (A{2,}) \n" +
                "  define \n" +
                "   A as A.value >= 50   " +
                ")";

        String epl2 = "@name('my-statement') select * from SpeedEvent\n" +
                "match_recognize (\n" +
                "  partition by key \n" +
                "  measures max(A.value) as value, B.value as b_value, first(A.key) as key \n" +
                "  after match skip to current row " +
                "  pattern (A{2,} B) \n" +
                "  define \n" +
                "   A as (A.value >= 30 and prev(A.value,1) is null) or (A.value > prev(A.value,1))," +
                "   B as true " +
                ")";

        String epl3 = "@name('my-statement') select * from SpeedEvent\n" +
                "match_recognize (\n" +
                "  partition by key \n" +
                "  measures max(A.value) as value, B.value as b_value, first(A.key) as key \n" +
                "  after match skip past last row " +
                "  pattern (A{2,} B) \n" +
                "  define \n" +
                "   A as  (prev(A.value,1) is null) or (Math.abs(A.value - A.firstOf().value) >= 2)," +
                "   B as true " +
                ")";

        String epl = "@name('my-statement') select * from SpeedEvent\n" +
                "match_recognize (\n" +
                "  partition by key \n" +
                "  measures max(A.value) as value, B.value as b_value, first(A.key) as key \n" +
                "  after match skip past last row " +
                "  pattern (A{2,} B) \n" +
                "  define \n" +
                "   A as  (prev(A.value,1) is null) or (A.aggregate(0, (result, res) => result + res.value) / A.countOf() > 67 )," +
                "   B as true " +
                ")";


        CompilerArguments args1 = new CompilerArguments(configuration);

        EPCompiled epCompiled;
        try {
            epCompiled = compiler.compile(epl, args1);
        } catch (EPCompileException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(configuration);

        EPEventService eventService = runtime.getEventService();
        eventService.clockExternal();

        EPDeployment deployment;
        try {
            deployment = runtime.getDeploymentService().deploy(epCompiled);
        } catch (EPDeployException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        EPStatement statement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "my-statement");

        statement.addListener((newData, oldData, s, r) -> {
            System.out.println(newData[0].getUnderlying());
        });

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Consumer"+ UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // ... set additional consumer properties (optional)
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);

        try {
            consumer.subscribe(Collections.singletonList("linear-road-data"));
            while (true) {
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(2));
                records.forEach(record -> {
                    String[] data = record.value().replace("[","").replace("]","").split(",");
                    long ts = Long.parseLong(data[8].trim());
                    runtime.getEventService().sendEventBean(new SpeedEvent(data[0].trim(), ts,Double.parseDouble(data[1].trim())), "SpeedEvent");
                });
            }
        } catch (WakeupException e) {
            // Using wakeup to close consumer
        } finally {
            consumer.close();
        }

    }

}
//b_value is the closing value
//value is the max in the match