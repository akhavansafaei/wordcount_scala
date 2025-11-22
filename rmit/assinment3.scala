package streaming

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

object assinment3 {

  def main(args: Array[String]): Unit = {
    val Conf = new SparkConf().setAppName("assignment3").setMaster("local[*]")

    val ssc = new StreamingContext(Conf, Seconds(3))

    ssc.checkpoint(".")

    val prefix = args(1)

    val input = ssc.textFileStream(args(0))

    var CounterA = 0
    var CounterB = 0
    var CounterC = 0

    // task A: word count and filtering input
    def taskA(input: DStream[String]): DStream[(String, Int)] = {
      val cleanWords = input.flatMap(_.split(" ")
        .filter { word =>
          word.matches("[a-zA-Z]+") &&
            word.length >= 3
        })

      val pairs = cleanWords
        .map((_, 1))
        .reduceByKey(_ + _)
      pairs
    }

    val wordCountFreq = taskA(input)

    wordCountFreq.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        CounterA += 1
        val suffixA = f"$CounterA%03d"
        val task_name = "taskA"
        val file2save = s"$prefix/$task_name-$suffixA"
        rdd.saveAsTextFile(file2save)
      }
    }

    // task B: co-Occurence frequency of words
    def taskB(input: DStream[String]): DStream[((String, String), Int)] = {
      val pairs = input.flatMap { Words =>
        val words = Words.split(" ")

        val cleanWords = words.filter { word =>
          word.matches("[a-zA-Z]+") && word.length >= 3
        }
        val wordPairs = for {i <- 0 until cleanWords.length
                             j <- i + 1 until cleanWords.length
                             } yield (cleanWords(i), cleanWords(j))
        wordPairs
      }
      val wordPairFreq = pairs.flatMap(pair => Seq(pair, pair.swap))
        .map(pair => (pair, 1))
        .reduceByKey(_ + _)
      wordPairFreq
    }

    val wordPairFreq = taskB(input)

    wordPairFreq.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        CounterB += 1
        val suffixB = f"$CounterB%03d"
        val task_name = "taskB"
        val file2save = s"$prefix/$task_name-$suffixB"
        rdd.saveAsTextFile(file2save)
      }
    }

    // task c: applying updatestatebykey using all of the rdds of dstream
    val updateFunction = (newValues: Seq[Int], pCount: Option[Int]) => {
      val now = newValues.sum
      val old = pCount.getOrElse(0)
      val newCount = old + now
      Some(newCount)
    }

    def taskC(input: DStream[String]): DStream[((String, String), Int)] = {
      val coWords = input.flatMap { Words =>
        val cleanWords = Words.split(" ").filter {
          words =>
            words.matches("[a-zA-Z]+") &&
              words.length >= 3
        }
        for {i <- 0 until cleanWords.length
             j <- (i + 1) until cleanWords.length}
        yield {
          ((cleanWords(i), cleanWords(j)), 1)
        }
      }
      val coWordsfreq = coWords.updateStateByKey(updateFunction)
      coWordsfreq
    }


    // val pairFrequency = taskC(input)
    val pairFrequency = taskC(input)

    // Function to save DStream as text files on HDFS with unique sequence numbers as suffixes
    def saveToHDFS(rdd: RDD[((String, String), Int)]): Unit = {
      if (!rdd.isEmpty()) {
        CounterC += 1
        val outputPath = s"$prefix/taskC-${"%03d".format(CounterC)}"
        rdd.saveAsTextFile(outputPath)
      }
    }

    // Output the pairFrequency DStream and save it to HDFS
    pairFrequency.foreachRDD(rdd => saveToHDFS(rdd))


    ssc.start() // Start the streaming context
    ssc.awaitTermination() // Wait for the streaming to terminate
  }

}
