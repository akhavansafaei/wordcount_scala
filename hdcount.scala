package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object hdcount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("WordCoOccurrence").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint(".") // Checkpoint directory for saving state data

    val inputDirectory = args(0)
    val outputDirectory = args(1)
    val lines = ssc.textFileStream(inputDirectory)

    // Function to update the co-occurrence frequency of words
    val updateFunction = (newValues: Seq[Int], state: Option[Int]) => {
      val currentCount = newValues.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    // Task C: Co-occurrence frequency calculation function
    def calculateCoOccurrence(lines: DStream[String]): DStream[((String, String), Int)] = {
      //val words = lines.flatMap(_.split(" ")).filter { word => word.matches("[a-zA-Z]+") && word.length > 2 }
      //val filteredWords = words.filter { word => word.matches("[a-zA-Z]+") && word.length > 2 }

      val coTerm = lines.flatMap { term =>
        val Words = term.split(" ").filter { word => word.matches("[a-zA-Z]+") && word.length > 2 }
        for {
          i <- 0 until Words.length
          j <- (i + 1) until Words.length
        } yield {
          ((Words(i).toString, Words(j).toString), 1)
        }
      }

      val coTermCounts = coTerm.updateStateByKey(updateFunction)
      coTermCounts
    }

    // task A

    var taskACounter = 1

    // Task C: Co-occurrence frequency calculation function
    def calculatewordCount(lines: DStream[String]): DStream[(String, Int)] = {
      val filteredWords=lines.flatMap(_.split(" ").filter { word => word.matches("[a-zA-Z]+") && word.length > 2 })

      val wordCount= filteredWords.map((_,1)).reduceByKey(_ + _)
      wordCount
    }
    val wordcounts=calculatewordCount(lines)
    wordcounts.foreachRDD { rdd =>
      // Check if the RDD is not empty before processing
      if (!rdd.isEmpty()) {
        // This block of code is executed for each non-empty RDD in the stream.


        // Save the non-empty RDD to HDFS with a unique sequence number for Task A
        val sequenceNumberA = f"$taskACounter%03d"
        val outputPathA = s"$outputDirectory/taskA-$sequenceNumberA"
        taskACounter += 1
        rdd.saveAsTextFile(outputPathA)
      }
    }



    // task B
    var taskBCounter = 1 // Initialize the taskBCounter
    def calculateCoOccurrenceB(lines: DStream[String]): DStream[((String, String), Int)] = {


        val coOccurrenceCountsPerLine = lines.flatMap { line =>
          val words = line.split(" ")

          // Filter out non-alphabetic and short words (< 3 characters)
          val filteredWords = words.filter { word =>
            word.matches("[a-zA-Z]+") && word.length >= 3
          }

          // Create pairs of filtered words for co-occurrence counting
          val coOccurrences = for {
            i <- 0 until filteredWords.length
            j <- i + 1 until filteredWords.length
          } yield (filteredWords(i), filteredWords(j))

          coOccurrences
        }

        // Calculate the co-occurrence frequency of word pairs using reduceByKey
        val wordPairCounts = coOccurrenceCountsPerLine
          .flatMap(pair => Seq(pair, pair.swap)) // Emit both directions of the pair
          .map(pair => (pair, 1))
          .reduceByKey(_ + _)
      wordPairCounts

      }
    val wordPairc=calculateCoOccurrenceB(lines)

    wordPairc.foreachRDD { rdd =>
      // Check if the RDD is not empty before processing
      if (!rdd.isEmpty()) {
        // This block of code is executed for each non-empty RDD in the stream.


        // Save the non-empty RDD to HDFS with a unique sequence number for Task A
        val sequenceNumberB = f"$taskBCounter%03d"
        val outputPathA = s"$outputDirectory/taskB-$sequenceNumberB"
        taskBCounter += 1
        rdd.saveAsTextFile(outputPathA)
      }
    }



    // task c
    var taskCCounter = 1 // Initialize the taskBCounter

    // Calculate co-occurrence frequency and save the output to HDFS
    val coOccurrenceCounts = calculateCoOccurrence(lines)
    val outputDirc = s"$outputDirectory/taskC"
    val sequenceNumberC = f"$taskCCounter%03d"
    val outputDirC = s"$outputDirc-$sequenceNumberC"
    taskCCounter += 1

    coOccurrenceCounts.saveAsTextFiles(outputDirC)
    lines.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val sequenceNumberC = f"$taskCCounter%03d"
        val outputDirC =s"$outputDirc-$sequenceNumberC"
        taskCCounter += 1
        coOccurrenceCounts.saveAsTextFiles(outputDirC)
        coOccurrenceCounts.print()

      }
    }

    ssc.start() // Start the streaming context
    ssc.awaitTermination() // Wait for the streaming to terminate
  }

}
