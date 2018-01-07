package NB;

import NaiveBayes.src.NaiveBayesTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main
{
  public Main() {}

  public static void main(String[] paramArrayOfString) throws Exception
  {
    // 处理传入配置文件【HDFS目录】，【训练集绝对/相对路径】，【测试集绝对/相对路径】，【输出目录】
    Configuration conf = new Configuration(); 
    String[] otherArgs = new GenericOptionsParser(conf, paramArrayOfString).getRemainingArgs();
    FileSystem fs = FileSystem.get(conf);
    Path path_train, path_temp, path_test, path_out;
    if(otherArgs.length != 4)
    {
      System.err.println("Usage: NaiveBayesMain <dfs_path> <train> <test> <out>");
      System.exit(2);
    }

    conf.set("train", otherArgs[0] + "/" +otherArgs[1]);
    conf.set("test", otherArgs[0] + "/" +otherArgs[2]);
    conf.set("output", otherArgs[0] + "/" +otherArgs[3]);
    conf.set("train_result", otherArgs[0] + "/" +otherArgs[1] + ".train");

    put2HDFS(otherArgs[1], otherArgs[0] + "/" + otherArgs[1], conf);
    put2HDFS(otherArgs[2], otherArgs[0] + "/" + otherArgs[2], conf);

    path_train = new Path(otherArgs[0] + "/" + otherArgs[1]);// 模型1输入
    path_temp = new Path(otherArgs[0] + "/" + otherArgs[1] + ".train"); // 缓存模型1和模型2之间的数据
    path_test = new Path(otherArgs[0] + "/" +otherArgs[2]); //模型2输入
    path_out = new Path(otherArgs[0] + "/" + otherArgs[3]); //模型2输出
    
    //模型1：处理训练集，Mapper对应训练集一行文本
    {
      // 配置Map-Reduce对应的类（方法）
      Job job_train = new Job(conf, "naive bayse training");
      job_train.setJarByClass(Main.class); // 处理类
      job_train.setMapperClass(FormalPossibility.FPMapper.class); //Mapper步骤，统计每一个句子【情感+词汇】
      job_train.setCombinerClass(FormalPossibility.FPReduce.class);//Combiner,Reduce步骤，统计训练集【情感+词汇】
      job_train.setReducerClass(FormalPossibility.FPReduce.class);
      job_train.setOutputKeyClass(Text.class);// 输出第一项为【情感+词汇】
      job_train.setOutputValueClass(IntWritable.class);//输出第二项为个数

      // 配置Map-Reduce对应的输入输出路径
      FileInputFormat.setInputPaths(job_train, path_train);
      if(fs.exists(path_temp)) // 存在路径则删除，否则会报错
        fs.delete(path_temp, true);
      FileOutputFormat.setOutputPath(job_train, path_temp);
      if(!job_train.waitForCompletion(true))
        System.exit(1);
    }
    //模型2：处理测试集，Mapper对应测试集一行文本
    {
      // 配置Map-Reduce对应的类（方法）
      Job job_test = new Job(conf, "naive bayse testing");
      job_test.setJarByClass(NBPredictor.class);            // setUp： 读取模型1输出
      job_test.setMapperClass(NBPredictor.NBMapper.class);   // Mapper步骤，使用NB对于输入句子预测
      job_test.setCombinerClass(NBPredictor.NBReducer.class); // Combiner，Reduce步骤：利用输入集合的Id进行排序输出
      job_test.setReducerClass(NBPredictor.NBReducer.class);
      job_test.setOutputKeyClass(IntWritable.class);
      job_test.setOutputValueClass(Text.class);
      // 配置Map-Reduce对应的输入输出路径
      FileInputFormat.setInputPaths(job_test, path_test);
      if(fs.exists(path_out))
        fs.delete(path_out, true);
      FileOutputFormat.setOutputPath(job_test, path_out);
      if(!job_test.waitForCompletion(true))
        System.exit(1);
      fs.delete(path_temp, true);
    }

    getFromHDFS(otherArgs[0] + "/" + otherArgs[3], ".", conf);

    fs.close();
    System.exit(0);
  }
  
  // 模型从HDFS存储文件
  public static void put2HDFS(String paramString1, String paramString2, Configuration paramConfiguration)
    throws Exception
  {
    Path localPath = new Path(paramString2);
    FileSystem localFileSystem = localPath.getFileSystem(paramConfiguration);
    
    localFileSystem.copyFromLocalFile(false, true, new Path(paramString1), new Path(paramString2));
  }
  // 模型从HDFS读取文件
  public static void getFromHDFS(String paramString1, String paramString2, Configuration paramConfiguration)
    throws Exception
  {
    Path localPath1 = new Path(paramString2);
    FileSystem localFileSystem = localPath1.getFileSystem(paramConfiguration);
    String[] arrayOfString = paramString1.split("/");
    Path localPath2 = new Path(arrayOfString[(arrayOfString.length - 1)]);
    if (localFileSystem.exists(localPath2)) {}
    localFileSystem.delete(localPath2, true);
    localFileSystem.copyToLocalFile(true, new Path(paramString1), localPath1);
  }
}
