package com.hp.bigdata.province;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hp.bigdata.utils.IPParser;
import com.hp.bigdata.utils.LogParser;

public class ProvinceApp {
	//提交
	public static void main(String[] args) throws Exception {
		//创建Configuration对象
		Configuration conf = new Configuration();
		//判断输出路径是否存在，存在删除
		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path("out");
		if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
		}
		//创建job对象
		Job job = Job.getInstance(conf);
		//设置提交类
		job.setJarByClass(ProvinceApp.class);
		//设置map相关信息
		job.setMapperClass(Mappers.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		//设置reduce相关信息
		job.setReducerClass(Reducers.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		//设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path("trackinfo_20130721.txt"));
		//���·�����벻����
		FileOutputFormat.setOutputPath(job, new Path("out"));
		//提交
		job.waitForCompletion(true);
	}
	static class Mappers extends Mapper<LongWritable, Text, Text, LongWritable>{
		//初始化属性
		private LogParser parser;
		//记录出现的次数  初始为1
		private LongWritable ONE = new LongWritable(1);
		//重写setup方法
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//实例化parser
			parser = new LogParser();
		}
		//重写map方法
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//读取一行数据
			String line = value.toString();
			//通过工具类解析
			Map<String,String> logInfo = parser.parse(line);
			//判断ip是否为空
			String ip = logInfo.get("ip");
			if(StringUtils.isNotBlank(ip)){
				//IP不为空       获取regionInfo对象
				IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp(ip);
				//获取省份信息
				String province = regionInfo.getProvince();
				if(StringUtils.isNotBlank(province)){
					//省份不能为空
					context.write(new Text(province), ONE);
				}else{
					context.write(new Text("--"), ONE);
				}
			}else{
				//IP为空
				context.write(new Text("--"), ONE);
			}
			}
	}
	//重写reducer方法
	static class Reducers extends Reducer<Text, LongWritable, Text, LongWritable>{
		@Override
		protected void reduce(Text province, Iterable<LongWritable> ones,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//定义空变量
			long sum = 0;
			//遍历得到每一个值并相加
			for (LongWritable one : ones) {
				sum++;
			}
			//写入上下文中
			context.write(province, new LongWritable(sum));
		}
	}

}
