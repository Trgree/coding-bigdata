package org.ace.spark.task;

/**
 * 启动spark程序
 * 封装spark任务，传递json格式的任务参数，生成spark任务
 * Created by Liangsj on 2018/3/30.
 */
public class Start {
    public static void main(String[] args) throws Exception {
        if(args.length !=2) {
            System.err.println("wrong args! usage: <paramJson> <WORKPATH>");
            return;
        }
        System.out.println("任务参数 param=" + args[0]);
        System.out.println("程序目录 WORKPATH=" + args[1]);
        System.setProperty("WORKPATH", args[1]);
        Processor processor = new Processor(args[0]);
        processor.process();
    }
}
