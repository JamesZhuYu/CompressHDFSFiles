package com.thenetcircle.tda.job;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class CompressHDFSFile {
    //压缩文件
    public static void compress(String inputPath,String outputpath) throws Exception{
    	//采用bz2的方式压缩
    	Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.BZip2Codec");
//        Class<?> codecClass = Class.forName(codecClassName);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
        //指定压缩文件路径
//        FSDataOutputStream outputStream = fs.create(new Path("/user/hadoop/text.gz"));
        FSDataOutputStream outputStream = fs.create(new Path(outputpath));
        //指定要被压缩的文件路径
//        FSDataInputStream in = fs.open(new Path("/user/hadoop/aa.txt"));
        FSDataInputStream in = fs.open(new Path(inputPath));
        //创建压缩输出流
        CompressionOutputStream out = codec.createOutputStream(outputStream);  
        IOUtils.copyBytes(in, out, conf); 
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
        
    }
    
    //解压缩
    public static void uncompress(String inputPath,String outputPath) throws Exception{
        Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.BZip2Codec");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
//        FSDataInputStream inputStream = fs.open(new Path("/user/hadoop/text.gz"));
        FSDataInputStream inputStream = fs.open(new Path(inputPath));
        FSDataOutputStream out = fs.create(new Path(outputPath));
         //把text文件里到数据解压，然后输出到控制台  
        InputStream in = codec.createInputStream(inputStream);  
        IOUtils.copyBytes(in, out, conf);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }
    
    //使用文件扩展名来推断二来的codec来对文件进行解压缩
    public static void uncompress(String uri) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        
        Path inputPath = new Path(uri);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(inputPath);
        if(codec == null){
            System.out.println("no codec found for " + uri);
            System.exit(1);
        }
        String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
        InputStream in = null;
        OutputStream out = null;
        try {
            in = codec.createInputStream(fs.open(inputPath));
            out = fs.create(new Path(outputUri));
            IOUtils.copyBytes(in, out, conf);
        } finally{
            IOUtils.closeStream(out);
            IOUtils.closeStream(in);
        }
    }
    
    public static void main(String[] args) throws Exception {
    	String hdfsPath = "hdfs://cloud-host-02:9000";
    	if(args.length < 2) {
    		System.err.println("Usage: type(compress|uncompress) inputPath outputPath");
    	}
    	
    	String type = args[0];
    	String inputPath = hdfsPath+args[1];
    	String outputPath = hdfsPath+args[2];
    	if(type.equals("compress")) {
    		compress(inputPath,outputPath);
    	} else {
    		uncompress(inputPath,outputPath);
    	}
    }

}
