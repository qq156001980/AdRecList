import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.wltea.analyzer.IKSegmentation;
import org.wltea.analyzer.Lexeme;
import org.apache.hadoop.mapreduce.Job;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;


/**
 * Created by Hunter on 14-10-11.
 */
public class MyIkAnalyzer {

    //读入需要分词的文件
    public static class Map extends Mapper<Object,Text,Text,Text>{
        //记录mac地址
        Text url=new Text();


        //Map函数
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            ArrayList<String> words=new ArrayList<String>();
            //读入一行文本
            String line=value.toString();
            String[] lineSeparate=line.split(" ");


            //遍历
            url.set(lineSeparate[0]);

            for(int i=1;i<lineSeparate.length;i++){
                words=separateWord(lineSeparate[i]);
                //写入context
                for(int j=0;j<words.size();j++){
                    context.write(url,new Text(words.get(j)));
                }

            }


        }
    }

    //去重
    public static class Combiner extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            ArrayList<String> wordList=new ArrayList<String>();
            for(Text value:values){
                String temp=value.toString();
                if(wordList.contains(temp)){
                    continue;
                }
                else {
                    wordList.add(temp);
                }
            }
            for(int i=0;i<wordList.size();i++){

                context.write(key,new Text(wordList.get(i)));
            }

        }
    }

    //写入文件
    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        //reduce函数
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value:values){
                context.write(key, value);
            }
        }
    }

    //分词函数
    public static ArrayList<String> separateWord(String line){
        IKSegmentation seg;
        StringReader reader=new StringReader(line);
        seg=new IKSegmentation(reader);
        ArrayList<String> words=new ArrayList<String>();
        Lexeme lex;

        try{
            while((lex=seg.next())!=null){
                String temp=lex.getLexemeText();
                if(words.contains(temp)){
                    continue;
                }
                else{
                    words.add(temp);
                }

            }
        }catch (IOException e){
            e.printStackTrace();
        }
        return words;
    }



}
