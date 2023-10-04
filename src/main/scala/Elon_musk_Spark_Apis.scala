
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.{SparkConf, SparkContext}


object Elon_musk_Spark_Apis {
  def main(args: Array[String]): Unit = {

   //hiding logging info
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    // creating conf object holding spark context information required in the driver application
    val conf = new SparkConf().
      setMaster("local").
      setAppName("Twitter spark apis ")

    // creating spark context
    val sc = new SparkContext(conf)
    // To avoid logging too many debugging messages
      sc.setLogLevel("ERROR")

    //read file
    val file_rdd = sc.textFile("src/main/data/elonmusk_tweets.csv")
    //println(file_rdd.count)

    //reading input - if theres no input , the trim will return an empty value
    println("Enter sentence ")
    val input_word = scala.io.StdIn.readLine().split(",").map(_.trim)

    //Requirement 1
    //map doc into tuples
    //split lines by date-text
    val date_rdd = file_rdd.map(line => {
      val key_value = line.split(",")
      (key_value(1), key_value(2))
    })
    //print date_rdd.foreach(println)

    //count input in tweet
    def countWord(t: String, x: Array[String]): Array[Int] =
    {
      x.map(k =>
        if (t.toLowerCase().contains(k.toLowerCase())) 1 else 0)
    }

    // Count how much that word is repeated in every tweet
    val n_OfInput_all_tweets = date_rdd.flatMap {
      case (date, text) =>
        val occurrences = countWord(text, input_word)
        input_word.indices.map(i => ((input_word(i), date), occurrences(i)))
    }

    //count how much that word is repeated in each date .
    val numberOf_inEveryDate = n_OfInput_all_tweets .reduceByKey(_ + _)

    // Print as tuples with the sentence and input word
    numberOf_inEveryDate.foreach {
      case ((input, date), count) =>
        println(s"($input, $date, $count)")
    }

    // Req 2
    val totalTweets = date_rdd.count()

    val at_least_one_input_tweets = date_rdd
      .filter {
            //filter by key
      case (_, text) =>
      { countWord(text, input_word)
        .sum > 0
      }
    }
    // function unit
    val in_percentage = (
      at_least_one_input_tweets
        .count()
        .toFloat / totalTweets.toFloat
      ) * 100

    //print result
    println(s"Tweets with at least one percentage is =  $in_percentage%")

    //Req 3
    // count word in rdd
    val two_words_in_tweet_count = date_rdd
      .filter {
        case (_, tweet) =>
          val counts = countWord(tweet, input_word)
          val sum = counts.sum
          sum == 2
      }
    // *100 to find the percentage
    val two_words_percentage =
      (two_words_in_tweet_count
      .count()
      .toFloat / totalTweets.toFloat) *100

    //result
    println(s"Tweets with 2 input percentage is =  $two_words_percentage%")


    // length - then mean for standard deviation equation
    //split with every word ( spacing )
    val rdd_length = date_rdd
      .map {
      case (_, text) =>
        {
          text
            .split(" ")
            .length
        }
    }

    // call functions
    val avg = rdd_length.mean()
    val std = rdd_length.stdev()
    println(s"The average =  $avg")
    println(s"Standard deviation  =  $std")



  }
}
