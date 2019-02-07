package pairs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * 自定义PairsTrial Writable类型
 * @author CaiLei
 *
 */
public class PairsTrial implements WritableComparable<PairsTrial>{

    private Text first;
    private Text second;

    /**
     * 默认的构造函数，这样MapReduce方法才能创建对象，然后通过readFeilds方法从序列化数据流中独处进行赋值
     */
    public PairsTrial() {
        set (new Text(), new Text());
    }

    public PairsTrial(String first, String second) {
        set(new Text(first), new Text(second));
    }

    public PairsTrial(Text first, Text second) {
        set(first, second);
    }

    public void set(Text first, Text second) {
        // TODO Auto-generated method stub
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    /**
     * 通过成员对象本身的readFeilds方法，从输入流中反序列化每一个成员对象
     * @param arg0
     * @throws IOException
     */
    @Override
    public void readFields(DataInput arg0) throws IOException {
        // TODO Auto-generated method stub
        first.readFields(arg0);
        second.readFields(arg0);
    }

    /**
     * 通过成员对象本身的write方法，序列化每一个成员对象到输出流中
     * @param arg0
     * @throws IOException
     */
    @Override
    public void write(DataOutput arg0) throws IOException {
        // TODO Auto-generated method stub
        first.write(arg0);
        second.write(arg0);
    }

    /**
     * 实现WritableComparable必须要实现的方法，用语比较排序
     * @param PairsTrial
     * @return
     */

    @Override
    public int compareTo(PairsTrial tp) {
        // TODO Auto-generated method stub
        int cmp = first.compareTo(tp.first);
        if(cmp != 0) {
            return cmp;
        }
        return second.compareTo(tp.second);
    }




    /**
     * 就像针对java语言构造任何值的对象，需要重写java.lang.Object中的hashCode(), equals()和toString()方法
     */

    /**
     * MapReduce需要一个Partitioner把map的输出作为输入分成一块块喂给多个reduce
     * 默认的是HashPartitioner，它是通过对象的hashCode函数进行分割，所以hashCode的好坏决定了分割是否均匀，它是一个关键的方法
     * @return
     */
    //当不使用reletive frequency时采用该hashCode求值方式
    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode(); 
    }



    @Override
    public boolean equals(Object o) {
        if(o instanceof PairsTrial) {
            PairsTrial tp = (PairsTrial) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }

    /**
     * 重写toString方法，作为TextOutputFormat输出格式的输出
     * @return
     */
    @Override
    public String toString() {
        return first + "," + second;
    }

    /**
     * 当PairsTrial被用作健时，需要将数据流反序列化为对象，然后再调用compareTo()方法进行比较。
     * 为了提升效率，可以直接对数据的序列化表示来进行比较
     */

    public static class Comparator extends WritableComparator {
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public Comparator() {
            super(PairsTrial.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                /**
                 * Text对象的二进制表示是一个长度可变的证书，包含字符串之UTF－8表示的字节数以及UTF－8字节本身。
                 * 读取该对象的起始长度，由此得知第一个Text对象的字节表示有多长；然后将该长度传给Text对象RawComparator方法
                 * 最后通过计算第一个字符串和第二个字符串恰当的偏移量，从而实现对象的比较
                 * decodeVIntSize返回变长整形的长度，readVInt表示文本字节数组的长度，加起来就是某个成员的长度
                 */
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);

                //先比较first
                int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
                if(cmp != 0) {
                    return cmp;
                }
                //再比较second
                return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1, b2, s2 + firstL2,  l2 - firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException();
            }
        }
    }

    //注册RawComparator, 这样MapReduce使用PairsTrial时就会直接调用Comparator
    static {
        WritableComparator.define(PairsTrial.class, new Comparator());
    }
    
    
    /**
    * 使用pair的方式，使用自定义了TextPiar Writable对象
    *
    */
    public static class Co_OccurrenceMatrixMapperWithPair extends Mapper<LongWritable, Text, PairsTrial, DoubleWritable> {

        @Override
        public void map(LongWritable inputKey, Text inputValue, Context context) 
                throws IOException, InterruptedException {

                String doc = inputValue.toString();
                //这里只是简单的根据正则分词，如果希望准确分词，请使用相关分词包
                String reg = "[\\p{P}\\s]";
                String[] allTerms = doc.split(reg);
                for(int i = 0; i < allTerms.length; i++) {
                    if((!"".equals(allTerms[i])) && allTerms[i] != null) {
                        //考虑in-mapper combining
                        Map<String, Integer> pairMap = new HashMap<String, Integer>();

                        //取出该单词对应的一定窗口大小内的共现词
                        String[] termNeighbors = neighborsOfTerm(allTerms[i], i, allTerms, 3);
                        for(String nbTerm : termNeighbors) {
                            if((!"".equals(nbTerm)) && nbTerm != null) {                        
                                String textPairStr = allTerms[i] + "," + nbTerm;
                                //in-mapper combining
                                if(!pairMap.containsKey(textPairStr)) {
                                    pairMap.put(textPairStr, 1);
                                } else {
                                    pairMap.put(textPairStr, pairMap.get(textPairStr) + 1);
                                }

                            }
                        }
                        for(Entry<String, Integer> entry: pairMap.entrySet()) {
                            String[] pairStrs = entry.getKey().split(",");
                            PairsTrial textPair = new PairsTrial(pairStrs[0], pairStrs[1]);
                            context.write(textPair, new DoubleWritable(entry.getValue()));  
                        }
                    }

                }

            }

        /**
        * 计算某个词在某窗口大小内的共现词
        * @param term
        * @param allterms
        * @return
        */
        public String[] neighborsOfTerm(String term, int pos, String[] allterms, int windowSize) {
                String[] neighbors = new String[windowSize];
                int count = allterms.length;
                int j = 0;
                int leftOffSet = 0;
                int rightOffSet = 0;
                if(pos < windowSize / 2) {
                    leftOffSet = pos;
                    rightOffSet = windowSize - leftOffSet;
                } else if (pos >= count - 1 - windowSize / 2) {
                    rightOffSet = count - 1 - pos;
                    leftOffSet = windowSize - rightOffSet;
                } else {
                    leftOffSet = windowSize / 2;
                    rightOffSet = windowSize - leftOffSet;
                }
                for(int i = pos - leftOffSet; i <= pos + rightOffSet && i >=0 && i < count; i++) {
                    if(term != allterms[i] ) {
                        neighbors[j] = allterms[i];
                        j ++;
                    }
                }

                return neighbors;
            }
        } 

    
    
    public static class Co_OccurrenceMatrixReducerWithPair extends Reducer<PairsTrial, DoubleWritable, PairsTrial, DoubleWritable> {
        @Override
        public void reduce(PairsTrial inputKey, Iterable<DoubleWritable> inputValues, Context context)
                throws IOException, InterruptedException {
                int sum = 0;
                for(DoubleWritable inC : inputValues) {
                    sum += inC.get();
                }
                context.write(inputKey, new DoubleWritable(sum));
            }
        }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{

        Configuration conf = new Configuration();
        String[] otherArgs= new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = Job.getInstance(conf);
        job.setJarByClass(PairsTrial.class); //设置启动作业类
        job.setMapperClass(Co_OccurrenceMatrixMapperWithPair.class); //设置Map类
        job.setReducerClass(Co_OccurrenceMatrixReducerWithPair.class);
        job.setMapOutputKeyClass(PairsTrial.class); //设置mapper输出的key类型
        job.setMapOutputValueClass(DoubleWritable.class); //设置mapper输出的value类型
        job.setNumReduceTasks(1); //设置Reduce Task的数量

        //设置mapreduce的输入和输出目录
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        //等待mapreduce整个过程完成
        System.exit(job.waitForCompletion(true)?0:1);
    }
    
}
