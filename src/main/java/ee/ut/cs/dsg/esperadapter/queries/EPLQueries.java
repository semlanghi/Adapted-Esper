package ee.ut.cs.dsg.esperadapter.queries;

public class EPLQueries {

    static final String epl1 = " create context SegmentedByKey partition by key from SpeedEvent; \n" +
            "  @name('ThresholdAbsolute')" +
            "  context SegmentedByKey " +
            "  select * from SpeedEvent\n" +
            "  match_recognize (\n" +
            "   partition by key" +
            "   measures first(A.timestamp) as first_ts, last(A.timestamp) as last_ts, max(A.value) as value, count(A.value) as num, first(A.key) as key\n" +
            "   after match skip past last row " +
            "   pattern (A{2,}) \n" +
            "  interval 10 seconds or terminated" +
            "   define \n" +
            "       A as A.value >= 50 " +
            ")";

    static final String epl2 =
            " create context SegmentedByKey partition by key from SpeedEvent; \n" +
            " @name('ThresholdRelative')" +
            " context SegmentedByKey " +
            " select * from SpeedEvent\n" +
            " match_recognize (\n" +
            "  partition by key \n" +
            "  measures max(max(A.value),C.value) as value, first(A.key) as key, last(A.timestamp) as last_ts, C.timestamp as first_ts, count(A.value) as cou \n" +
            "  after match skip past last row " +
            "  pattern (C A+) \n" +
            "  interval 5 seconds or terminated" +
            "  define \n" +
            "   C as C.value >= 30," +
            "   A as (A.value > C.value * 0.1) and (A.value > prev(A.value,1) + (prev(A.value,1)*0.1))" +
            ")";

    static String epl3 =
            "   create schema ResultStream (value double, first_ts long, last_ts long, key string)" +
                    "starttimestamp first_ts endtimestamp last_ts;\n"+
                    "   create context SegmentedByKey2 partition by key from SpeedEvent, key from ResultStream; \n" +
                    "   context SegmentedByKey2 create variable long last_time2 = 0L;" +
                    "   context SegmentedByKey2 insert into ResultStream select * from SpeedEvent#expr(timestamp>last_time2)\n" +
            "   match_recognize (\n" +
            "   partition by key \n" +
            "   measures (sum(A.value)+C.value)/(count(A.value)+1) as value, C.timestamp as first_ts, last(A.timestamp) as last_ts, first(A.key) as key \n" +
                    " after match skip past last row "+
                    "   pattern (C A+) \n" +
                    "  interval 5000 seconds or terminated" +
            "   define \n" +
            "       A as (Math.abs(A.value - C.value) >= 5)" +
            ");\n" +
                    "   context SegmentedByKey2 on ResultStream(first_ts>last_time2) set last_time2 = last_ts; " +
                    "   @name('Delta') select * from ResultStream;"
            ;

    public static final String epl =
            "   create context SegmentedByKey partition by key from SpeedEvent; \n" +
            "   context SegmentedByKey " +
            "   insert into ResultStream select avg(value) as value, first(timestamp) as first_ts, last(timestamp) as last_ts, key as key, count(*) as count  from SpeedEvent#expr_batch(avg(value) < 67.0,false) ;" +
                    "   @name('Aggregate')" +
                    "   select value, first_ts, last_ts, key from ResultStream(value>=67.0 and count>=2)" ;

    public static String query(String name){
        switch (name){
            case "ThresholdAbsolute": return epl1;
            case "ThresholdRelative": return epl2;
            case "Delta": return epl3;
            case "Aggregate": return epl;
            default: return null;
        }
    }
}
