This is Scala codes with Spark to search target word and find the document with highest TF score and corresponding TF score. main() will call getTF() to create a RDD(named as joined in the codes) in this format: (word, List((fileName1, TermFrequencyScore1),(fileName2, TermFrequencyScore2)…)). Using this RDD, we will get the docuemnt with highest TF score and corresponding TF score.

I made it a Maven project using IntelliJ IDEA, so I think to run this codes, you just need to pull it from GitHub and run TermFrequency.scala (under src/main/scala/TermFrequency). The sample documents are in data folder. The output result is in result.txt.

Github:https://github.com/Aoweid/TermFrequencyScore-ScalaWithSpark

Chitty chatty digression:
I used to write java and python with Elastic Search, but noticing this is not a difficult task, I decided to learn Scala and Spark in these two days and tried to write the codes to implement the function. I was just surprised by how powerful and elegant Scala and Spark can be, so I am changing one current code for document classification in my current part-time work to use Scala and Spark. Quite interesting process, I learned a lot from this experience and impoved another project on hand, just wanna say thank you.
