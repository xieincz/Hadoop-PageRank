import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class PageRankIter {
    private static final double d = 0.85;// 阻尼系数

    public static class Map extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            // 输入文件格式： a b,c,d(first) 采用KeyValueTextInputFormat
            // a,pr b,c,d(next)
            String[] out = value.toString().split(",");// 外链
            String[] link = key.toString().split(",");// next
            double pr = 1.0;// 初始pr值
            if (link.length > 1) {
                // next
                pr = Double.parseDouble(link[1]);
            }
            int outNum = out.length;// a的出链数
            // 输出格式<a的各出链:b/c/d, a;pr;outNum>
            for (String s : out) {
                context.write(new Text(s), new Text(link[0] + ";" + pr + ";" + outNum));
            }
            // <a, out>以便迭代处理
            context.write(new Text(link[0]), value);// 输出 key:本网站 value:出链
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double pr = (double) (1.0 - d);// 储存PageRank值
            String[] str;
            Text outLinks = new Text();// 记录该链接的所有出链信息
            // 集合的数据位key的所有入链链接的page,rank,count值，以及key的所有出链信息
            for (Text t : values) {
                // 入链信息以';'分割，出链信息以','分割，以此区别
                str = t.toString().split(";");
                if (str.length == 3) {// t的形式 <a的一个出链,a;pr;a的出链数>
                    // 计算key的rank值=(1-d)+d*key的入链rank值/其出链数
                    pr += Double.parseDouble(str[1]) / Integer.parseInt(str[2]) * d;
                } else {// 说明接受到的value是此key的所有出链
                    outLinks.set(t.toString()); // <a, b,c,d>
                }
            }
            context.write(new Text(key.toString() + "," + pr), outLinks);// 更新key网站的pr
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        if (args.length != 2) {
            System.err.println("ERROR");
            System.exit(2);
        }
        Path path = new Path(args[1]);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(new Path(args[1]))) {
            fileSystem.delete(new Path(args[1]), true);
        }
        Job job = Job.getInstance(conf, "PageRankIter");
        job.setJarByClass(PageRankIter.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        // 设置Map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // 设置Reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}

class PageRankViewer {
    // 10次迭代后，按格式输出 要按pr值排序，保留10位小数
    public static class Map extends Mapper<Text, Text, DoubleWritable, Text> {
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = key.toString();// key的形式 a,pr value的形式b,c,d(出链)
            String url = line.split(",")[0];
            String pr = line.split(",")[1];
            // 用负号处理可以利用key的自动排序然后再取负号得到从大到小的效果
            context.write(new DoubleWritable(-Double.valueOf(pr)), new Text(url));
        }
    }

    public static class Reduce extends Reducer<DoubleWritable, Text, NullWritable, Text> {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                Double pr = -Double.valueOf(key.toString());// 因为前面map加了负号
                String result = "(" + value.toString() + "," + String.format("%.10f", pr) + ")";// 保留十位小数
                context.write(null, new Text(result));// key:null value:(url,pr)
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        if (args.length != 2) {
            System.err.println("ERROR");
            System.exit(2);
        }
        Path path = new Path(args[1]);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(new Path(args[1]))) {
            fileSystem.delete(new Path(args[1]), true);
        }
        Job job = Job.getInstance(conf, "PageRankViewer");
        job.setJarByClass(PageRankViewer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        // 设置Map输出类型
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        // 设置Reduce输出类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}

public class PageRank_Hadoop {
    private static int times = 10;// 迭代次数

    public static void main(String[] args) throws IllegalArgumentException, Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        // conf.set("fs.defaultFS", "hdfs://10.102.0.198:9000");
        args = new String[] { "hdfs://localhost:9000/ex3/input",
                "hdfs://localhost:9000/ex3/Experiment_3_Hadoop" };
        // args = new String[] { "hdfs://10.102.0.198:9000/ex3/input", "hdfs://10.102.0.198:9000/user/bigdata_202000202092/Experiment_3_Hadoop" };
        Path path = new Path(args[1]);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(new Path(args[1]))) {
            fileSystem.delete(new Path(args[1]), true);
        }
        String temp = "hdfs://localhost:9000/Temp";// 临时文件目录，用于存放每次迭代的结果
        // String temp = "hdfs://10.102.0.198:9000/Temp";
        if (fileSystem.exists(new Path(temp))) {
            fileSystem.delete(new Path(temp), true);
        }
        fileSystem.mkdirs(new Path(temp));
        String[] forItr = { args[0], temp + "/Data1" };
        PageRankIter.main(forItr);// 第一次迭代
        for (int i = 1; i < times; ++i) {// 后面几次的迭代
            forItr[0] = temp + "/Data" + i;
            forItr[1] = temp + "/Data" + String.valueOf(i + 1);
            PageRankIter.main(forItr);
        }
        String[] forRV = { temp + "/Data" + times, args[1] };
        PageRankViewer.main(forRV);// 输出最终结果
        fileSystem.delete(new Path(temp), true);// 删除临时文件目录(即10次迭代的输入输出目录)
    }
}
