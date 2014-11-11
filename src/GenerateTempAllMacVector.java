import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by Hunter on 14/11/11.
 */
public class GenerateTempAllMacVector {


    //划分向量字符串
    public static HashMap<String,String> splitVString(String s){
        HashMap<String,String> hashMap2=new HashMap<String, String>();
        String[] vw=s.split(" ");
        for(String value:vw){
            if(!("".equals(value))){
                String []temp=value.split(":");
                hashMap2.put(temp[0],temp[1]);
            }
        }
        return hashMap2;
    }


    public static class Map extends Mapper<Text,Text,Text,Text> {



        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString().trim();
            HashMap<String,String> hashMap1=splitVString(line);
            Set setKW1=hashMap1.keySet();
            Iterator iteratorKW=setKW1.iterator();
            while(iteratorKW.hasNext()){
                String keyWord1=iteratorKW.next().toString();
                String weight=hashMap1.get(keyWord1);
                context.write(new Text(key.toString()+":"+keyWord1),new Text(weight));

            }

        }
    }


    //合并map存入的结果
    public static class Reduce extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String line=key.toString().trim();
            String [] mk=line.split(":");
            int sum=0;
            for(Text value:values){
                int i=Integer.parseInt(value.toString().trim());
                sum+=i;
            }
            context.write(new Text(mk[0]),new Text(mk[1]+":"+sum));
        }
    }



}
