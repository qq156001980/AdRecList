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

import java.awt.*;
import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * Created by Hunter on 14-10-13.
 */
public class OutputVector {

    //读入关键词列表到hashmap函数
    public static HashMap<String,ArrayList<String>> inputHashMap(String path){
        System.out.println("this is inputHashMap.........");
        HashMap<String,ArrayList<String>> wordsMap=new HashMap<String, ArrayList<String>>();
        Configuration conf=new Configuration();
        BufferedReader in=null;
        try{
            FileSystem fs=FileSystem.get(URI.create(path),conf);
            FSDataInputStream fin=fs.open(new Path(path));
            String line;
            in=new BufferedReader(new InputStreamReader(fin,"UTF-8"));
            while ((line=in.readLine())!=null){

                //保存关键词
                String key=null;

                //关键词所包含的词语
                ArrayList<String> wordsList=new ArrayList<String>();

                //控制关键词的读取
                int flag=0;

                //遍历没一行包含的词语
                for(String value:line.split(" ")){

                    //第一个词语保存为key
                    if(flag==0){
                        key=value;
                        flag=1;
                        continue;
                    }


                    //第二个词语开始为关键词语列表
                    if(!("".equals(value))){
                        wordsList.add(value);
                    }
                }
                wordsMap.put(key,wordsList);


            }
            return wordsMap;
        }catch(IOException e){

            e.printStackTrace();
            return null;
        }finally {
            if(in!=null){
                try{
                    in.close();
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
        }

    }

    //读入分词文件
    public static class Map extends Mapper<Text,Text,Text,Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            context.write(key,value);
        }
    }

    //统计用户访问网页的关键词进行输出
    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        private static String path="keyword/keyword.txt";
        private static HashMap<String,ArrayList<String>> wordsMap=inputHashMap(path);

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            HashMap<String,Integer> vectorMap=new HashMap<String,Integer>();


            //遍历关键词列表

            for(Text value:values){
                //得到关键词集合
                Set s=wordsMap.keySet();

                //生成迭代器
                Iterator it=s.iterator();

                //关键词
                String keyWord=null;

                while (it.hasNext()){
                    //权重
                    int weight=0;

                    //取出一个关键词
                    keyWord=(String)it.next();

                    //取出一个关键词所包含的词语数组
                    ArrayList<String> wordList=wordsMap.get(keyWord);

                    if(wordList.contains(value.toString())){
                        weight++;
                    }
                    if(weight!=0){
                        if(vectorMap.keySet().contains(keyWord)){
                            weight+=vectorMap.get(keyWord).intValue();
                            vectorMap.put(keyWord,new Integer(weight));

                        }else{
                            vectorMap.put(keyWord,new Integer(weight));
                        }

                    }

                }

            }

            //遍历向量map输出到文件
            Set vs=vectorMap.keySet();

            Iterator itr=vs.iterator();

            String vector="";
            while (itr.hasNext()){
                String temp=(String)itr.next();
                vector+=temp+":";
                vector+=vectorMap.get(temp).toString()+" ";
            }
            if("".equals(vector)){
                return;
            }
            context.write(key,new Text(vector));



        }

    }
    
}
