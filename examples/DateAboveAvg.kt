package org.example

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.*
import java.io.IOException
import java.util.*


class DateMapper : MapReduceBase(), Mapper<LongWritable?, Text, Text?, IntWritable?> {
    @Throws(IOException::class)
    override fun map(
        key: LongWritable?, value: Text,
        output: OutputCollector<Text?, IntWritable?>,
        reporter: Reporter
    ) {
        val line = value.toString()
        val s = StringTokenizer(line, " ")
        val year = s.nextToken()

        var lastToken: String? = null
        while (s.hasMoreTokens()) {
            lastToken = s.nextToken()
        }

        if (lastToken != null) {
            val avgPrice = lastToken.toInt()
            output.collect(Text(year), IntWritable(avgPrice))
        } else {
            System.err.println("Input line does not contain enough tokens: $line")
        }
    }
}



class DateReducer : MapReduceBase(), Reducer<Text?, IntWritable, Text?, IntWritable?> {
    @Throws(IOException::class)
    override fun reduce(
        key: Text?, values: Iterator<IntWritable>,
        output: OutputCollector<Text?, IntWritable?>, reporter: Reporter
    ) {
        val maxAvg = 30
        var value: Int

        while (values.hasNext()) {
            if ((values.next().get().also { value = it }) > maxAvg) {
                output.collect(key, IntWritable(value))
            }
        }
    }
}


object Main {
    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val conf = JobConf(Main::class.java)
        conf.jobName = "max_eletricityunits"
        conf.outputKeyClass = Text::class.java
        conf.outputValueClass = IntWritable::class.java
        conf.mapperClass = DateMapper::class.java
        conf.combinerClass = DateReducer::class.java
        conf.reducerClass = DateReducer::class.java
        conf.setInputFormat(TextInputFormat::class.java)
        conf.setOutputFormat(TextOutputFormat::class.java)

        FileInputFormat.setInputPaths(conf, Path(args[0]))
        FileOutputFormat.setOutputPath(conf, Path(args[1]))

        JobClient.runJob(conf)
    }
}