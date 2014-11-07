import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Hunter on 14/11/7.
 */
public class GenerateMiddleMacVector {

    public static class Map extends Mapper<Text,Text,Text,Text>{

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }

    }

    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String temp="";
            for(Text value:values){
                temp+=value.toString()+" ";
            }
            context.write(key,new Text(temp));
        }
    }

}
