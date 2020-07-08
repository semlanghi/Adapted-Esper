package esper.util;

import java.io.*;

public class Projector {

    public static void main(String[] args){
        FileReader reader = null;
        try {
            reader = new FileReader("/Users/samuelelanghi/Desktop/linear2.csv");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String focus = "[319,";
        File file = new File("Projection-"+focus.substring(1, focus.length()-1));
        FileWriter writer = null;
        try {
            writer = new FileWriter(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (BufferedReader bufferedReader = new BufferedReader(reader)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                // ... do something with line
                if(line.contains(focus)){
                    writer.write(line+"\n");
                    writer.flush();
                }

            }
        } catch (IOException e) {
            // ... handle IO exception
        }
    }
}
