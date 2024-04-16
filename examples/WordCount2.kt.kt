import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import java.io.IOException
import kotlin.system.exitProcess

object `WordCount2.kt` {
    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val conf = Configuration()
        val job = Job.getInstance(conf, "MapRed Introduction")

        job.setJarByClass(WordCount::class.java)
        job.mapperClass = WordCountMapper::class.java
        job.reducerClass = WordCountReducer::class.java
        job.outputKeyClass = Text::class.java
        job.outputValueClass = Text::class.java

        FileInputFormat.setInputPaths(job, Path(args[0]))
        FileOutputFormat.setOutputPath(job, Path(args[1]))

        exitProcess(if (job.waitForCompletion(true)) 0 else 1)
    }

    class WordCountMapper : Mapper<LongWritable?, Text, Text?, Text?>() {
        private var valueOutFilename: Text? = null

        override fun setup(ctx: Context) {
            val inputSplit: InputSplit = ctx.inputSplit
            val fileSplit = inputSplit as FileSplit
            fileSplit.start
            val filePath = fileSplit.path
            val fileName = filePath.name
            valueOutFilename = Text(fileName)
        }

        @Throws(IOException::class, InterruptedException::class)
        override fun map(
            keyIn: LongWritable?,
            valueIn: Text,
            ctx: Context
        ) {
            for (word in StringUtils.split(valueIn.toString())) {
                ctx.write(Text(word), valueOutFilename)
            }
        }
    }

    class WordCountReducer : Reducer<Text?, Text, Text?, Text?>() {
        @Throws(IOException::class, InterruptedException::class)
        public override fun reduce(
            keyInWord: Text?,
            valuesInFileNames: Iterable<Text>,
            context: Context
        ) {
            val fileNamesUnique = HashSet<String>()

            for (fileName in valuesInFileNames) {
                fileNamesUnique.add(fileName.toString())
            }

            val fileNamesOut: String = java.lang.String(StringUtils.join(fileNamesUnique, " - ")).toString()
            context.write(keyInWord, Text(fileNamesOut))
        }
    }
}