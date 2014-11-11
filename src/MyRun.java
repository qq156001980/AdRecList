import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Hunter on 14/11/7.
 */
public class MyRun {


    //输入输出路径
    private static String in1;
    private static String out1="temp/wordseparate";
    private static String out2="temp/urlvector";
    private static String out3="temp/tempmacvector";
    private static String out4="temp/midddlemacvector";
    private static String out5="temp/finalmacvector";
    private static String out6="temp/reclist";
    private static String out7="temp/alltempmacvector";
    private static String out8="temp/allmacvector";


    private static String macurl="macurl/macurl.txt";
    private static String keyword="keyword/keyword.txt";
    private static String macvector="macvector/macvector.txt";
    private static String ad="ad/ad.txt";


    public static String getMacurl() {
        return macurl;
    }

    public static void setMacurl(String urlword) {
        MyRun.macurl = urlword;
    }

    public static String getKeyword() {
        return keyword;
    }

    public static void setKeyword(String keyword) {
        MyRun.keyword = keyword;
    }

    public static String getMacvector() {
        return macvector;
    }

    public static void setMacvector(String macvector) {
        MyRun.macvector = macvector;
    }

    public static String getAd() {
        return ad;
    }

    public static void setAd(String ad) {
        MyRun.ad = ad;
    }

    //主函数
    public static void main(String[] Args) throws Exception {

        in1=Args[0];

        Configuration conf=new Configuration();

        if(Args.length!=1){
            System.err.println("Please use in");

            System.exit(2);
        }

        //MyIkAnalyzer
        Job job1=Job.getInstance(conf);

        job1.setJarByClass(MyIkAnalyzer.class);

        //设置map and reduce 处理类
        job1.setMapperClass(MyIkAnalyzer.Map.class);

        job1.setCombinerClass(MyIkAnalyzer.Combiner.class);

        job1.setReducerClass(MyIkAnalyzer.Reduce.class);

        //设置map输出类型
        job1.setMapOutputKeyClass(Text.class);

        job1.setMapOutputValueClass(Text.class);

        //设置reduce输出类型

        job1.setOutputKeyClass(Text.class);

        job1.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job1, new Path(in1));

        FileOutputFormat.setOutputPath(job1, new Path(out1));

        job1.waitForCompletion(true);



        //OutputVector
        Job job2=Job.getInstance(conf);

        job2.setJarByClass(OutputVector.class);

        //设置map and reduce 处理类
        job2.setMapperClass(OutputVector.Map.class);

        job2.setReducerClass(OutputVector.Reduce.class);

        //设置map输出类型
        job2.setMapOutputKeyClass(Text.class);

        job2.setMapOutputValueClass(Text.class);

        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        //设置reduce输出类型
        job2.setOutputKeyClass(Text.class);

        job2.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job2, new Path(out1));

        FileOutputFormat.setOutputPath(job2, new Path(out2));

        job2.waitForCompletion(true);



        //GenerateTempVector
        Job job3=Job.getInstance(conf);

        job3.setJarByClass(GenerateTempVector.class);

        //设置map and reduce 处理类
        job3.setMapperClass(GenerateTempVector.Map.class);

        job3.setCombinerClass(GenerateTempVector.Combiner.class);

        job3.setReducerClass(GenerateTempVector.Reduce.class);

        //设置map输出类型
        job3.setMapOutputKeyClass(Text.class);

        job3.setMapOutputValueClass(Text.class);

        job3.setInputFormatClass(KeyValueTextInputFormat.class);

        //设置reduce输出类型
        job3.setOutputKeyClass(Text.class);

        job3.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job3, new Path(out2));

        FileOutputFormat.setOutputPath(job3, new Path(out3));

        job3.waitForCompletion(true);




        //GenerateMiddleVector
        Job job4=Job.getInstance(conf);

        job4.setJarByClass(GenerateMiddleMacVector.class);

        //设置map and reduce 处理类
        job4.setMapperClass(GenerateMiddleMacVector.Map.class);

        job4.setReducerClass(GenerateMiddleMacVector.Reduce.class);

        //设置map输出类型
        job4.setMapOutputKeyClass(Text.class);

        job4.setMapOutputValueClass(Text.class);

        job4.setInputFormatClass(KeyValueTextInputFormat.class);

        //设置reduce输出类型
        job4.setOutputKeyClass(Text.class);

        job4.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job4, new Path(out3));

        FileOutputFormat.setOutputPath(job4, new Path(out4));

        job4.waitForCompletion(true);



        //FinalMacVector
        Job job5=Job.getInstance(conf);

        job5.setJarByClass(FinalMacVector.class);

        //设置map and reduce 处理类
        job5.setMapperClass(FinalMacVector.Map.class);

        job5.setCombinerClass(FinalMacVector.Combiner.class);

        job5.setReducerClass(FinalMacVector.Reduce.class);

        //设置map输出类型
        job5.setMapOutputKeyClass(Text.class);

        job5.setMapOutputValueClass(Text.class);

        job5.setInputFormatClass(KeyValueTextInputFormat.class);

        //设置reduce输出类型
        job5.setOutputKeyClass(Text.class);

        job5.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job5, new Path(out4));

        FileOutputFormat.setOutputPath(job5, new Path(out5));

        job5.waitForCompletion(true);







        //GenerateAdList
        Job job6=Job.getInstance(conf);

        job6.setJarByClass(GenerateAdList.class);

        //设置map and reduce 处理类
        job6.setMapperClass(GenerateAdList.Map.class);

        job6.setCombinerClass(GenerateAdList.Combiner.class);

        job6.setReducerClass(GenerateAdList.Reduce.class);

        //设置map输出类型


        job6.setMapOutputValueClass(Text.class);

        job6.setInputFormatClass(KeyValueTextInputFormat.class);

        //设置reduce输出类型
        job6.setOutputKeyClass(Text.class);

        job6.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job6, new Path(out5));

        FileOutputFormat.setOutputPath(job6, new Path(out6));

        job6.waitForCompletion(true);



        //Generatetempallmacvector
        Job job7=Job.getInstance(conf);

        job7.setJarByClass(GenerateTempAllMacVector.class);


        //设置map and reduce 处理类



        job7.setMapperClass(GenerateTempAllMacVector.Map.class);

        job7.setReducerClass(GenerateTempAllMacVector.Reduce.class);

        //设置map输出类型


        job7.setMapOutputValueClass(Text.class);

        job7.setInputFormatClass(KeyValueTextInputFormat.class);

        //设置reduce输出类型
        job7.setOutputKeyClass(Text.class);

        job7.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job7, new Path(out5));

        FileInputFormat.addInputPath(job7, new Path("macvector"));

        FileOutputFormat.setOutputPath(job7, new Path(out7));

        job7.waitForCompletion(true);


        //Generateallmacvector
        Job job8=Job.getInstance(conf);

        job8.setJarByClass(GenerateAllMacVector.class);


        //设置map and reduce 处理类



        job8.setMapperClass(GenerateAllMacVector.Map.class);

        job8.setReducerClass(GenerateAllMacVector.Reduce.class);

        //设置map输出类型


        job8.setMapOutputValueClass(Text.class);

        job8.setInputFormatClass(KeyValueTextInputFormat.class);

        //设置reduce输出类型
        job8.setOutputKeyClass(Text.class);

        job8.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job8, new Path(out7));

        FileOutputFormat.setOutputPath(job8, new Path(out8));


        System.exit(job8.waitForCompletion(true) ? 0 : 1);

    }

}
