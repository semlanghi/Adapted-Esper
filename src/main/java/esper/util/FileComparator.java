package esper.util;

import java.io.*;
import java.util.Objects;
import java.util.logging.Logger;

public class FileComparator {

    static Logger LOGGER = Logger.getLogger(FileComparator.class.getName());
    static boolean flinkToEsper = true;

    public static void main(String[] args){
        String prefix = "result/";
        File flinkFile = new File(prefix+"Output-Flink-Delta-exp3.txt");
        File esperFile = new File(prefix+"Output-Esper-Delta-exp5.txt");
        FileWriter diffWriter = null;
        try {
            if(!flinkToEsper)
                diffWriter = new FileWriter("/result/Missing-rows.txt");
            else diffWriter = new FileWriter("/result/Missing-rows-reverse.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            FileReader flinkReader = new FileReader(flinkFile);
            FileReader esperReader = new FileReader(esperFile);
            BufferedReader flinkBufferedReader = new BufferedReader(flinkReader);
            BufferedReader esperBufferedReader = new BufferedReader(esperReader);

            try {
                String firstLine, secondLine;
                boolean found = false;
                while ((firstLine = (flinkToEsper ? flinkBufferedReader : esperBufferedReader).readLine()) != null) {

                    ComparativeElement firstElem = flinkToEsper ? flinkElem(firstLine) : esperElem(firstLine);

                    while ((secondLine = (flinkToEsper ? esperBufferedReader : flinkBufferedReader).readLine()) != null && !found) {
                        ComparativeElement secondElem = flinkToEsper ? esperElem(secondLine) : flinkElem(secondLine);
                        if(firstElem.equals(secondElem))
                            found = true;
                    }

                    if(!found){
                        assert diffWriter != null;
                        diffWriter.write(firstElem.toString()+"\n");
                        System.out.println(firstElem.toString());
                    }
                    if(!flinkToEsper)
                        flinkBufferedReader = new BufferedReader(new FileReader(flinkFile));
                    else esperBufferedReader = new BufferedReader(new FileReader(esperFile));
                    found = false;
                }

                esperBufferedReader.close();
                esperReader.close();
                flinkBufferedReader.close();
                flinkReader.close();
                assert diffWriter != null;
                diffWriter.close();
            } catch (IOException e) {
                // ... handle IO exception
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


    }

    private static ComparativeElement esperElem(String esperLine){
        String[] partLine = esperLine.replace("{", "").replace("}","").split(", ");
        return new ComparativeElement(Long.parseLong(partLine[3].substring(9)), Long.parseLong(partLine[0].substring(8)), Integer.parseInt(partLine[2].substring(4)));
    }

    private static ComparativeElement flinkElem(String flinkLine){
        String[] partLine2 = flinkLine.replace("SpeedDeltaInterval","").replace("(","").replace(")","").split(", ");
        return new ComparativeElement(Long.parseLong(partLine2[0].substring(7)), Long.parseLong(partLine2[1].substring(5)), Integer.parseInt(partLine2[4].substring(5)));
    }

    private static class ComparativeElement{
        private long start;
        private long end;
        private int key;

        public ComparativeElement(long start, long end, int key) {
            this.start = start;
            this.end = end;
            this.key = key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ComparativeElement)) return false;
            ComparativeElement that = (ComparativeElement) o;
            return start == that.start &&
                    end == that.end &&
                    key == that.key;
        }

        @Override
        public int hashCode() {
            return Objects.hash(start, end, key);
        }

        @Override
        public String toString() {
            return "ComparativeElement{" +
                    "start=" + start +
                    ", end=" + end +
                    ", key=" + key +
                    '}';
        }
    }
}
