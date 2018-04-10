package org.ace.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.ace.utils.StringUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * spark 1.6
 */
public class SparkSql implements Serializable{
	private static final long serialVersionUID = 1L;
	public String input = "hdfs://localhost:8020/project/tw_user_info_http/100";
	public String output ="hdfs://localhost:8020/project/sparksql_output/100";;
	
	public static void main(String[] args) {
		SparkSql spark = new SparkSql();
		spark.start();
	}
	
	public void start(){
		SparkConf sparkConf = new SparkConf().setAppName("spark sql example");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(jsc);
		JavaRDD<String> source = jsc.textFile(input);
		// 定义表字段
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("userid", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("attcode", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("attvalue", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("frequency", DataTypes.FloatType, true));
		fields.add(DataTypes.createStructField("days", DataTypes.FloatType, true));
		fields.add(DataTypes.createStructField("lastdate", DataTypes.StringType, true));
		StructType schema = DataTypes.createStructType(fields);
		JavaRDD<Row> rowRDD = source.map(new Function<String, Row>() {
		     private static final long serialVersionUID = 1L;
		     @Override
		     public Row call(String record) throws Exception {
		           String[] fields = record.split("\\|");
		           if(fields.length < 6){
		                return RowFactory.create("", "", "", 0, 0,"");
		           }
		           return RowFactory.create(fields[0].trim(), fields[1].trim(), fields[2].trim(), StringUtil.str2float(fields[3].trim(),0), StringUtil.str2float(fields[4].trim(),0), fields[5].trim());
		     }
		}).filter(new Function<Row, Boolean>() {
		     private static final long serialVersionUID = 1L;
		     @Override
		     public Boolean call(Row row) throws Exception {
		           return row.getString(1).equals("APP_T") && row.getFloat(3)>0  &&  row.getFloat(4) >0;
		     }
		});

		// 在中2.10不适用
		/*// 创建DF
		DataFrame userInfoDataFrame = sqlContext.createDataFrame(rowRDD, schema);
		userInfoDataFrame.registerTempTable("userInfo");
		userInfoDataFrame.show();
		// 计算
		DataFrame max = sqlContext.sql("SELECT attvalue, max(frequency) maxfreq,max(days) maxday,min(frequency) minfreq,min(days) minday  FROM userInfo   group by attvalue");
		max.show();
		//输出到hdfs
		max.javaRDD().map(new Function<Row, String>() {
		     private static final long serialVersionUID = 1L;
		     @Override
		     public String call(Row v1) throws Exception {
		           return v1.toString();
		     }
		}).saveAsTextFile(output);
*/
	}
}
