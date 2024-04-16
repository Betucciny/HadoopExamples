

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

class WordCountMapper : Mapper<Any, Text, Text, IntWritable>() {
    private val one = IntWritable(1)
    private val word = Text()

    override fun map(key: Any?, value: Text?, context: Context?) {
        value?.toString()?.split("\\s".toRegex())?.forEach {
            word.set(it)
            context?.write(word, one)
        }
    }
}

class WordCountReducer : Reducer<Text, IntWritable, Text, IntWritable>() {
    override fun reduce(key: Text?, values: MutableIterable<IntWritable>?, context: Context?) {
        val sum = values?.sumOf { it.get() } ?: 0
        context?.write(key, IntWritable(sum))
    }
}


object Main {
    @JvmStatic
    fun main(args: Array<String>) {
        val conf = Configuration()
        val job = Job.getInstance(conf, "word count")
        job.setJarByClass(Main::class.java)

        job.mapperClass = WordCountMapper::class.java
        job.combinerClass = WordCountReducer::class.java
        job.reducerClass = WordCountReducer::class.java

        job.outputKeyClass = Text::class.java
        job.outputValueClass = IntWritable::class.java

        val inputPath = Path(args[0])
        val outputPath = Path(args[1])
        println(inputPath)
        println(outputPath)
        FileInputFormat.addInputPath(job, inputPath)
        FileOutputFormat.setOutputPath(job, outputPath)

        System.exit(if (job.waitForCompletion(true)) 0 else 1)
    }
}
