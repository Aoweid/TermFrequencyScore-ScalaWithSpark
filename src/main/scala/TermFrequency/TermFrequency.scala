package TermFrequency

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * This is scala codes with spark to get term frequency from data text.
  * main() will call getTF() to operate the function and write the result into one result.txt
  * The getTF() takes data files folder path and target words list, then return a string list contains the term frequency
  * results for each word in the target words list
  *
  * Created by Aowei on 3/12/17.
  */
object TermFrequency {
  /**
    * main() calls getTF() to operate the function, then write the results into one txt file
   */
  def main(args: Array[String]){
    val documentDir = "data"
    val wordList: List[String] = List("queequeg", "whale", "sea")
    val res: List[String] = getTF(documentDir, wordList)
    // write result to result.txt
    val writer = new PrintWriter(new File("result.txt" ))
    res.foreach(x=>writer.write(x+"\n"))
    writer.close()
  }

  /**
    * getTF() will use input documentDir path to get all data files and use the content to construct RDD with Spark.
    * The final RDD(named as joined in the codes) will be in this format:
    * (word, List((fileName1, TermFrequencyScore),(fileName2, TermFrequencyScore)...))
    * and the list is sorted by TermFrequency descending, then fileName
    * Example: (account,List((mobydick-chapter1.txt,8.920606601248885E-4), (mobydick-chapter3.txt,1.6711229946524063E-4)))
    * Based on this RDD, look up each word in the target words list input and store the information of the name of the
    * document with highest term frequency score and corresponding tf score into a string list, then return this string
    * list as output
    * @param documentDir, path to the folder of the data documents
    * @param wordList, a list contains the target words that need to return related document with highest tf score and
    *                corresponding tf score
    * @return List[String], a List contains json strings shows the result of each word
    */
  def getTF(documentDir: String, wordList: List[String]): List[String] ={
    //initiate spark with local[2]
    val conf = new SparkConf().setAppName("TermFrequency").setMaster("local[2]").set("spark.executor.memory","1g")
    val sc = new SparkContext(conf)
    //first step to get a RDD ((word, fileName),1) for all words.
    val step1 = sc.wholeTextFiles(documentDir).
      flatMap{ case ( filePath, contents) => // nothing, just first time using scala and I can't help saying this case feature in scala is so elegant compared to JAVA LOL
        val words = contents.split("""\W+""").filter(word => word.length>0)//split by W+, in the same time all punctuations get removed
        val fileName = filePath.split(File.separator).last
        words.map(word=>( (word.toLowerCase,fileName) , 1 ))//use lower case
      }
    //get the total number of words in each document for future TF score calculation, create a RDD (fileName, count)
    val totalWordsInFiles = step1.map{ case  ((word,fileName),count) => (fileName,count) }.reduceByKey(_+_)
    //operate word counting after step1 RDD, and create a step2 RDD ( fileName,(word , count )) for future join
    val step2 = step1.reduceByKey(_+_).map{case ( (word,fileName) , count ) => ( fileName,(word , count ))}
    // join totalWordsInFiles and step2 to calculate the TF scores
    val joined = step2.join(totalWordsInFiles).map{ case(fileName,(tuple, total )) =>
      (tuple._1, (fileName,tuple._2/total.toDouble)) // join two RDDs to this format (word, (fileName, TFScore))
    }
      .groupByKey() // group by word
      .sortByKey(ascending = true)
      .mapValues{iterable=>
        val vec = iterable.toVector.sortBy{ case (fileName, tf) =>
          (-tf,fileName) //sort by TermFrequency descending, then fileName
        }
        vec.toList
      }
    var res = new ListBuffer[String]()
    //for each target word, look it up in joined RDD to get the document with highest TF score and corresponding TF score
    wordList.foreach(x =>
      // no such word in RDD
      if(joined.lookup(x).isEmpty) {
        val documentName = "Not Found"
        val tf = 0
        res+=s"""{"Word":"$x","DocumentName":"$documentName","TermFrequency":$tf}"""
      }
        // found the word in RDD
      else{
        val hightestTF = joined.lookup(x).head.head._2
        for(tuple <- joined.lookup(x).head){// in case there may be more than one document has the highest TF score, if so, store all of them
          if(tuple._2==hightestTF){
            val documentName = tuple._1
            val tf = tuple._2
            res+=s"""{"Word":"$x","DocumentName":"$documentName","TermFrequency":$tf}"""
          }
        }
      }
    )
    sc.stop()
    res.toList
  }
}
