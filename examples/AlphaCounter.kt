package org.example

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import java.io.IOException
import kotlin.system.exitProcess


class AlphaMapper : Mapper<Any?, Text, Text?, LongWritable?>() {
    private val character = Text()

    @Throws(IOException::class, InterruptedException::class)
    public override fun map(key: Any?, value: Text, context: Context) {
        val v = value.toString()
        for (i in v.indices) {
            character.set(v.substring(i, i + 1))
            context.write(character, one)
        }
    }

    companion object {
        private val one = LongWritable(1)
    }
}

class AlphaReducer : Reducer<Text?, LongWritable, Text?, LongWritable?>() {
    private val result = LongWritable()

    @Throws(IOException::class, InterruptedException::class)
    public override fun reduce(key: Text?, values: Iterable<LongWritable>, context: Context) {
        var sum: Long = 0
        for (`val` in values) {
            sum += `val`.get()
        }
        result.set(sum)
        context.write(key, result)
    }
}


object AlphaCounter : Configured(){
    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val conf = Configuration()
        val job = Job.getInstance(conf, "Alpha Counter Job")

        job.setJarByClass(AlphaCounter::class.java)
        job.mapperClass = AlphaMapper::class.java
        job.reducerClass = AlphaReducer::class.java
        job.outputKeyClass = Text::class.java
        job.outputValueClass = LongWritable::class.java
        job.inputFormatClass = TextInputFormat::class.java
        job.outputFormatClass = TextOutputFormat::class.java

        FileInputFormat.addInputPath(job, Path(args[0]))
        FileOutputFormat.setOutputPath(job, Path(args[1]))

        exitProcess(if (job.waitForCompletion(true)) 0 else 1)
    }
}