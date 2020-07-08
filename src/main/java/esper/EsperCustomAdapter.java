package esper;

import com.espertech.esper.common.internal.collection.Pair;

import java.util.function.Function;

public interface EsperCustomAdapter<V,E> {
    public void process(Function<V, Pair<E,Long>> transformationFunction);
}
