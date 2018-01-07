package NB;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Map;
import java.util.StringTokenizer;

// 计算先验概率，统计词频和感情频率，与wordcount类似。
public class FormalPossibility {

  // Mapper: (K1, V1) --> list(K2, v2)
  // (LineId, RowText) -> ((words,tf), emotion)
  public static class FPMapper extends Mapper<Object, Text, Text, IntWritable> {

    final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context)

            throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());
      // Text formor: emotion + words

      // count the emotion
      String emotion = itr.nextToken();

      // count the text with emotion
      while (itr.hasMoreTokens()) {
        String now = itr.nextToken();
        if(now.charAt(0) >= 'a' && now.charAt(0) <= 'z')
        {
          word.set(emotion+now);
          context.write(word, one);
        }
      }
    }
  }

  // Reducer: (K2, list(V2))--> list(K3, V3)
  // (countValueName, countValue)
  public static class FPReduce  extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text words, Iterable<IntWritable> Cnt, Context context)

            throws IOException, InterruptedException {
      int val = 0;
      for(IntWritable v : Cnt){
        val += v.get();
      }

      context.write(words, new IntWritable(val));

    }

  }
}


