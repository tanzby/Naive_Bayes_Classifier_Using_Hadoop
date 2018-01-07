package NB;

import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class NBPredictor
{
  public NBPredictor() {}
  
  public static class NBMapper extends Mapper<Object, Text, IntWritable, Text>
  {
    final String LOW = "LOW";
    final String MID = "MID";
    final String HIG = "HIG";
    public FPReader train;
    
    public NBMapper() {}
    
    public void setup(Context paramMapper) {
      try { Configuration localConfiguration = paramMapper.getConfiguration();
        // setup会在每个Mapper之前执行，初始化数据。
        train = new FPReader();
        train.getData(localConfiguration.get("train_result"), localConfiguration);
      }
      catch (Exception localException)
      {
        localException.printStackTrace();
        System.exit(1);
      }
    }
    
    public void map(Object paramObject, Text paramText, Context paramMapper)
      throws java.io.IOException, InterruptedException
    {
      try
      {
        StringTokenizer localStringTokenizer = new StringTokenizer(paramText.toString());
        IntWritable sentenceID = new IntWritable(Integer.parseInt(localStringTokenizer.nextToken()));
        double alpha = 1.0E-7D;
        // 计算每个类别自身的概率
        double p_low = Math.log(alpha + (Integer) train.emoF.get("LOW") * 1.0D / train.emoF
          .size());
        double p_mid = Math.log(alpha + (Integer) train.emoF.get("MID") * 1.0D / train.emoF
          .size());
        double p_high = Math.log(alpha + (Integer) train.emoF.get("HIG") * 1.0D / train.emoF
          .size());
        // P(Emo|words):=\sum（log（P(EMO|WORD)))
        while (localStringTokenizer.hasMoreTokens()) {
          String word = localStringTokenizer.nextToken();
          if ((word.charAt(0) >= 'a') && (word.charAt(0) <= 'z'))
          {
            p_low += Math.log((Integer) train.freq.getOrDefault("LOW" + word, 0) + alpha) - Math.log((Integer)train.emoF.get("LOW")  + train.worF.size()*alpha);
            p_mid += Math.log((Integer) train.freq.getOrDefault("MID" + word,0) + alpha) - Math.log((Integer)train.emoF.get("MID")  + train.worF.size()*alpha);
            p_high += Math.log((Integer) train.freq.getOrDefault("HIG" + word,0) + alpha) - Math.log((Integer)train.emoF.get("HIG")  + train.worF.size()*alpha);
          }
        }
        String label = "???";
        // 选取最高的作为预测结果
        if ((p_low >= p_mid) && (p_low >= p_high)) label = "LOW";
        if ((p_mid >= p_low) && (p_mid >= p_high)) label = "MID";
        if ((p_high >= p_low) && (p_high >= p_mid)) label = "HIG";
        //输出， 传递参数
        paramMapper.write(sentenceID, new Text(label));
      } catch (Exception localException) {
        localException.printStackTrace();
      }
    }
  }
  
  public static class NBReducer
    extends Reducer<IntWritable, Text, IntWritable, Text>
  {
    public NBReducer() {}
    
    public void reduce(IntWritable sentenceID, Iterable<Text> Sentence, Context paramReducer) throws java.io.IOException, InterruptedException
    {
      Iterator localIterator = Sentence.iterator();
      if (localIterator.hasNext()) {
        Text localText = (Text)localIterator.next();
        paramReducer.write(sentenceID, localText);
        return;
      }
    }
  }
}
