package org.ace.hbase.mr;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * Created by Liangsj on 2018/4/4.
 */
public class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {

    public static enum Counters{
        ROWS
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        value.listCells().forEach( cell -> {
            if(cell.getValueLength() > 0){
               context.getCounter(Counters.ROWS).increment(1);
            }
        });
    }
}
