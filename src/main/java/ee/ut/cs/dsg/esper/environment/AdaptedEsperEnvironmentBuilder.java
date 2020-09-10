package ee.ut.cs.dsg.esper.environment;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import ee.ut.cs.dsg.esper.d2ia.EPLQueries;
import ee.ut.cs.dsg.esper.environment.adapters.EsperCustomAdapterConfig;
import ee.ut.cs.dsg.esper.util.LoggingListener;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * Builder class to create {@link AdaptedEsperEnvironment} instances.
 * It gives the possibility to add statements, and attach the relative listener.
 *
 * @param <K> The type of the optional key of the event
 * @param <V> The type of the event coming ut of the source
 * @param <E> The type of the event sent to Esper
 */
public class AdaptedEsperEnvironmentBuilder<K,V,E> {

    private final EPCompiler compiler;
    private final Configuration configuration;
    private final Map<String, EPCompiled> compiledStatementList;
    private final Properties props;

    public AdaptedEsperEnvironmentBuilder(Properties props) {
        compiler = EPCompilerProvider.getCompiler();
        configuration = new Configuration();
        this.props = props;
        compiledStatementList = new HashMap<>();
    }

    public AdaptedEsperEnvironmentBuilder<K,V,E> withMapType(String name, Map<String,Object> attributes){
        configuration.getCommon().addEventType(name, attributes);
        return this;
    }

    // TODO: add supports for multiple types, associating one mapping fucntion for each
    public AdaptedEsperEnvironmentBuilder<K,V,E> withBeanType(Class<E> name){
        configuration.getCommon().addEventType(name);
        return this;
    }

    public AdaptedEsperEnvironmentBuilder<K,V,E> addStatement(String stmtName){
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

    public AdaptedEsperEnvironment<V,E> buildRuntime(boolean externalClock){
        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(configuration);
        if(externalClock)
            runtime.getEventService().clockExternal();

        for (String tempName: compiledStatementList.keySet()
        ) {
            deployStatement(tempName, runtime);
        }

        return new AdaptedEsperEnvironment<>(runtime);
    }

    public AdaptedEsperEnvironment<V,E> buildRuntime(boolean externalClock, boolean registerPerf){
        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(configuration);
        if(externalClock)
            runtime.getEventService().clockExternal();

        for (String tempName: compiledStatementList.keySet()
             ) {
            deployStatement(tempName, runtime);
        }

        return new AdaptedEsperEnvironment<>(runtime, registerPerf);
    }

    private void deployStatement(String statementName, EPRuntime runtime){
        EPDeployment deployment;
        try {
            deployment = runtime.getDeploymentService().deploy(compiledStatementList.get(statementName));
        } catch (EPDeployException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        attachLoggingListener(runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), statementName));
    }

    private void attachLoggingListener(EPStatement statement){

        // Creating the output log file
        File temp = new File("result/outputs/");
        try {
            if(!temp.exists()){
                temp.createNewFile();
                temp = new File("Output-Esper-"+statement.getName()+"-"
                        +props.getProperty(EsperCustomAdapterConfig.EXPERIMENT_ID)+".txt");
            }

            // Attaching the listener to the statement
            statement.addListener(new LoggingListener(temp));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
