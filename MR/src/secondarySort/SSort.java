package secondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class SSort {
	//自己定义的key类应该实现WritableComparable接口
    public static class IntPair implements WritableComparable<IntPair>
    {
        int first;
        int second;
        /**
         * Set the left and right values.
         */
        public void set(int left, int right)
        {
            first = left;
            second = right;
        }
        public int getFirst()
        {
            return first;
        }
        public int getSecond()
        {
            return second;
        }
        
        //反序列化，从流中的二进制转换成IntPair
        public void readFields(DataInput in) throws IOException
        {
            // TODO Auto-generated method stub
            first = in.readInt();
            second = in.readInt();
        }
        
        //序列化，将IntPair转化成使用流传送的二进制
        public void write(DataOutput out) throws IOException
        {
            // TODO Auto-generated method stub
            out.writeInt(first);
            out.writeInt(second);
        }
        
        //key的比较
        public int compareTo(IntPair o)
        {
            // TODO Auto-generated method stub
            if (first != o.first)
            {
                return first < o.first ? -1 : 1;
            }
            else if (second != o.second)
            {
                return second < o.second ? -1 : 1;
            }
            else
            {
                return 0;
            }
        }

        //新定义类应该重写的两个方法
        @Override
        public int hashCode()
        {
            return first * 157 + second;
        }
        @Override
        public boolean equals(Object right)
        {
            if (right == null)
                return false;
            if (this == right)
                return true;
            if (right instanceof IntPair)
            {
                IntPair r = (IntPair) right;
                return r.first == first && r.second == second;
            }
            else
            {
                return false;
            }
        }
    }
    
    /**
     * 分区函数类。根据first确定Partition。
     */
   public static class FirstPartitioner extends Partitioner<IntPair, IntWritable>
   {
       @Override
       public int getPartition(IntPair key, IntWritable value,int numPartitions)
       {
           return Math.abs(key.getFirst() * 127) % numPartitions;
       }
   }

   /**
    * 分组函数类。只要first相同就属于同一个组。
    */
   public static class GroupingComparator extends WritableComparator
   {
       protected GroupingComparator()
       {
           super(IntPair.class, true);
       }
       @Override
       //比较两个 WritableComparables.
       public int compare(WritableComparable w1, WritableComparable w2)
       {
           IntPair ip1 = (IntPair) w1;
           IntPair ip2 = (IntPair) w2;
           int l = ip1.getFirst();
           int r = ip2.getFirst();
           return l == r ? 0 : (l < r ? -1 : 1);
       }
   }


   // 自定义map
   public static class Map extends Mapper<LongWritable, Text, IntPair, IntWritable>
   {
       private final IntPair intkey = new IntPair();
       private final IntWritable intvalue = new IntWritable();
       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
       {
           String line = value.toString();
           StringTokenizer tokenizer = new StringTokenizer(line);
           int left = 0;
           int right = 0;
           if (tokenizer.hasMoreTokens())
           {
               left = Integer.parseInt(tokenizer.nextToken());
               if (tokenizer.hasMoreTokens())
                   right = Integer.parseInt(tokenizer.nextToken());
               intkey.set(left, right);
               intvalue.set(right);
               context.write(intkey, intvalue);
           }
       }
   }
   // 自定义reduce
   //
   public static class Reduce extends Reducer<IntPair, IntWritable, Text, IntWritable>
   {
       private final Text left = new Text();
       private static final Text SEPARATOR = new Text("------------------------------------------------");
       
       public void reduce(IntPair key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
       {
           context.write(SEPARATOR, null);
           left.set(Integer.toString(key.getFirst()));
           for (IntWritable val : values)
           {
               context.write(left, val);
           }
       }
   }
   /**
    * @param args
    */
   public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
   {
       // 读取hadoop配置
       Configuration conf = new Configuration();
       // 实例化一道作业
       org.apache.hadoop.mapreduce.Job job = org.apache.hadoop.mapreduce.Job.getInstance(conf);
       job.setJarByClass(SSort.class);
       // Mapper类型
       job.setMapperClass(Map.class);
       // Reducer类型
       job.setReducerClass(Reduce.class);
       // 分区函数
       job.setPartitionerClass(FirstPartitioner.class);
       // 分组函数
       job.setGroupingComparatorClass(GroupingComparator.class);

       // map 输出Key的类型
       job.setMapOutputKeyClass(IntPair.class);
       // map输出Value的类型
       job.setMapOutputValueClass(IntWritable.class);
       // rduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
       job.setOutputKeyClass(Text.class);
       // rduce输出Value的类型
       job.setOutputValueClass(IntWritable.class);

       // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。
       job.setInputFormatClass(TextInputFormat.class);
       // 提供一个RecordWriter的实现，负责数据输出。
       job.setOutputFormatClass(TextOutputFormat.class);

       // 输入hdfs路径
       FileInputFormat.setInputPaths(job, new Path("/shaoshuai/input/words.txt"));
       // 输出hdfs路径
       FileOutputFormat.setOutputPath(job, new Path("/shaoshuai/output/ssres2"));
       // 提交job
       System.exit(job.waitForCompletion(true) ? 0 : 1);
   }


}
