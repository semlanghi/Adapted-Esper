package esper;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class KafkaAdaptedEsperEnvironmentBuilder<K,V,E> {

    private EPCompiler compiler;
    private Configuration configuration;
    private Map<String, EPCompiled> compiledStatementList;
    private Properties props;

    public KafkaAdaptedEsperEnvironmentBuilder(Properties props) {
        compiler = EPCompilerProvider.getCompiler();
        configuration = new Configuration();
        this.props = props;
        //configuration.getCompiler().getByteCode().setAccessModifiersPublic();
        //configuration.getCompiler().getByteCode().setBusModifierEventType(EventTypeBusModifier.BUS);
        compiledStatementList = new HashMap<>();
    }

    public KafkaAdaptedEsperEnvironmentBuilder<K,V,E> withMapType(String name, Map<String,Object> attributes){
        configuration.getCommon().addEventType(name, attributes);
        return this;
    }

    public KafkaAdaptedEsperEnvironmentBuilder<K,V,E> withBeanType(Class<E> name){
        configuration.getCommon().addEventType(name);
        return this;
    }

    public KafkaAdaptedEsperEnvironmentBuilder<K,V,E> addStatement(String stmtName){
        String stmt = EPLQueries.query(stmtName);
        CompilerArguments compilerArguments = new CompilerArguments(configuration);

        try {
            EPCompiled epCompiled = compiler.compile(stmt, compilerArguments);
            compiledStatementList.put(stmtName, epCompiled);
        } catch (EPCompileException ex) {
            // handle exception here
            ex.printStackTrace();
        }
        return this;
    }

    public EsperEnvironment<K,V,E> buildRuntime(boolean externalClock){
        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(configuration);
        if(externalClock)
            runtime.getEventService().clockExternal();

        for (String tempName: compiledStatementList.keySet()
             ) {
            deployStatement(tempName, runtime);
        }

        return new EsperEnvironment(runtime);
    }

    private void deployStatement(String statementName, EPRuntime runtime){
        EPDeployment deployment;
        try {
            deployment = runtime.getDeploymentService().deploy(compiledStatementList.get(statementName));
        } catch (EPDeployException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        EPStatement statement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), statementName);


        File temp = new File("result/outputs/");
        if(temp.exists()){
            temp = new File("Output-Esper-"+statementName+"-"
                    +props.getProperty(EsperCustomAdapterConfig.EXPERIMENT_ID)+".txt");
        }else{
            temp = new File("Output-Esper-"+statementName+"-"
                    +props.getProperty(EsperCustomAdapterConfig.EXPERIMENT_ID)+".txt");
        }
        try {

            File finalTemp = temp;
            statement.addListener(new UpdateListener() {

                final Logger LOGGER = Logger.getLogger(UpdateListener.class.getName());
                long count = 1;
                final FileWriter writer = new FileWriter(finalTemp);
                @Override
                public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {
                    try {
                        writer.write(newEvents[0].getUnderlying().toString()+"\n");
                        writer.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    //LOGGER.info("Outputted "+ count++ +"th event.");
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
