import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by Hunter on 14-10-25.
 */
public class GenerateTempVector {

    //读入macid,url列表
    public static HashMap<String,ArrayList<String>> inputMacUrl(String path){
        HashMap<String,ArrayList<String>> macUrl=new HashMap<String, ArrayList<String>>();
        BufferedReader in=null;
        try{
            Configuration conf=new Configuration();
            FileSystem file=FileSystem.get(conf);
            FSDataInputStream fin=file.open(new Path(path));
            String line;
            in=new BufferedReader(new InputStreamReader(fin,"UTF-8"));
            while((line=in.readLine())!=null){
                ArrayList<String> list=new ArrayList<String>();
                String []strings=line.trim().split(" ");
                String macID1=strings[0];

                for(int i=1;i<strings.length;i++){
                    list.add(strings[i]);
                }

                if(!("".equals(macID1))){
                    macUrl.put(macID1,list);
                }
            }
            return macUrl;

        }catch(IOException e){
            e.printStackTrace();
            return null;
        }
    }

    //读入macid,vector列表
    public static HashMap<String,HashMap<String,String>> inputMacVector(String path){
       HashMap<String,HashMap<String,String>> macVector=new HashMap<String, HashMap<String, String>>();


        BufferedReader in=null;

        try{
            Configuration conf=new Configuration();
            FileSystem file=FileSystem.get(conf);
            FSDataInputStream fin=file.open(new Path(path));
            String line;
            in=new BufferedReader(new InputStreamReader(fin,"UTF-8"));
            while((line=in.readLine())!=null){
                HashMap<String,String> hashMap1=new HashMap<String, String>();
                String []strings=line.trim().split(" ");
                String macID2=strings[0];
                for(int i=1;i<strings.length;i++){
                    String []s=strings[i].split(":");
                    hashMap1.put(s[0],s[1]);
                }

                if(!("".equals(macID2))){
                    macVector.put(macID2,hashMap1);
                }
            }
            return macVector;

        }catch(IOException e){
            e.printStackTrace();
            return null;
        }
    }

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


    public static class Map extends Mapper<Text,Text,Text,Text>{
        private static String path="macurl/macurl.txt";

        private static HashMap<String,ArrayList<String>> macUrl=inputMacUrl(path);

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            String line=value.toString().trim();
            //储存关键词权重
            Set setMU=macUrl.keySet();
            Iterator iteratorMU=setMU.iterator();

            while(iteratorMU.hasNext()){
                String temp=(String)iteratorMU.next();
                if(macUrl.get(temp).contains(key.toString())){
                    HashMap<String,String> hashMap3=splitVString(line);
                    Set setKW=hashMap3.keySet();
                    Iterator iteratorKW=setKW.iterator();
                    while(iteratorKW.hasNext()){
                        String keyWord=iteratorKW.next().toString();
                        String weight=hashMap3.get(keyWord);
                        context.write(new Text(temp+":"+keyWord),new Text(weight));

                    }


                }


            }

        }
    }

    //合并map存入的结果
    public static class Combiner extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String line=key.toString().trim();
            String [] mk=line.split(":");
            int sum=0;
            for(Text value:values){
                int i=Integer.parseInt(value.toString().trim());
                sum+=i;
            }
            context.write(new Text(mk[0]+":"+mk[1]),new Text(sum+""));
        }
    }




    public static class Reduce extends Reducer<Text,Text,Text,Text>{
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
