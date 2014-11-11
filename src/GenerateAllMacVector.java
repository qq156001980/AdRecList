import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Hunter on 14/11/11.
 */
public class GenerateAllMacVector {

    public static class Map extends Mapper<Text,Text,Text,Text> {

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }

    }

    public static class Reduce extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String vector="";
            for(Text value:values){
                vector+=value.toString();
                vector+=" ";
            }

            context.write(key,new Text(vector));

        }
    }
}
