/* SimpleApp.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.SparkConf
import java.io._

import org.apache.spark.sql.SparkSession

/*
val spark = SparkSession
  .builder()
  .appName("Spark SQL Example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

// For implicit conversions like converting RDDs to DataFrames
import spark.implicits._
*/

import scala.xml._
import scala.util.Try


object SimpleApp {

  abstract class Record {
    val postID: String = ""
    val ups: Int = -1
    val downs: Int = -1
    val favs: Int = -1
    val userID: String = ""
    val rep: Int = -1

    /**
      * Defines addition operation for different types of records
      *
      * @param that VoteRecord to add to this VoteRecord
      * @return new VoteRecord with sum of ups and downs
      */
    def +[T <: Record](that: T) = (this, that) match {
      case (PostRecord(_, _), PostRecord(_, _)) =>
        new PostRecord(
          this.postID,
          this.favs + that.favs
        )
      case (VoteRecord(_, _, _), VoteRecord(_, _, _)) =>
        new VoteRecord(
          this.postID,
          this.ups + that.ups,
          this.downs + that.downs
        )
      case (OutRecord(_, _, _), OutRecord(_, _, _)) =>
        new OutRecord(
          this.favs + that.favs,
          this.ups + that.ups,
          this.downs + that.downs
        )
      case (UserRecord(_, _), UserRecord(_, _)) =>
        new UserRecord(
          this.userID,
          this.rep + that.rep
        )
      case _ => throw new java.util.NoSuchElementException("cannot combine")
    }
  }

  case object EmptyRecord extends Record


  case class PostRecord private(override val postID: String,
                                override val favs: Int) extends Record {
    def this(t: (String, Int)) = this(t._1, t._2)

    def this(line: String) = this({
      val elem = XML.loadString(line)
      val postID: String = elem \ "@Id" text
      val favs: Int = (elem \ "@FavoriteCount" text).toInt
      val out = (postID, favs)
      out
    })
  }


  case class VoteRecord(override val postID: String,
                        override val ups: Int,
                        override val downs: Int) extends Record {
    def this(t: (String, Int, Int)) = this(t._1, t._2, t._3)

    def this(line: String) = this({
      val elem = XML.loadString(line)

      val postID = elem \ "@PostId" text
      val voteID = elem \ "@VoteTypeId" text

      if (voteID == "2") {
        (postID, 1, 0)
      } else if (voteID == "3") {
        (postID, 0, 1)
      } else {
        throw new java.util.InputMismatchException("Vote is not up/down vote.")
      }
    })

  }

  case class UserRecord(override val userID: String,
                        override val rep: Int) extends Record

  case object UserRecord extends Record {
    def apply(line: String) = {
      val elem = XML.loadString(line)

      val userID = elem \ "@Id" text
      val rep = (elem \ "@Reputation" text).toInt
      val out = new UserRecord(userID, rep)
      out
    }
  }

  case class OutRecord(override val favs: Int,
                       override val ups: Int,
                       override val downs: Int) extends Record {
    def this(t: Tuple3[Int, Int, Int]) = this(t._1, t._2, t._3)

    /**
      * Auxilliary constructor
      *
      * @param pRec a PostRecord instance
      * @param vRec a VoteRecord instance
      * @return a new OutRecord instance
      */
    def this(pRec: Record, vRec: Record) = this({
      (pRec, vRec) match {
        case (p: PostRecord, v: VoteRecord) => (p.favs, v.ups, v.downs)
        case _ =>
          throw new java.util.InputMismatchException("Cannot combine records.")
      }
    })
  }


  /**
    * Parses a line of a XML to generate a Record subtype
    *
    * @param line       the string corresponding to a single input line
    * @param recordType specifies the type of record to generate
    * @return a VoteRecord instance
    */
  def recordParser(line: String, recordType: String): Record =
    recordType match {
      case "post" => Try(new PostRecord(line)) getOrElse EmptyRecord
      case "vote" => Try(new VoteRecord(line)) getOrElse EmptyRecord
      case _ => throw new java.util.InputMismatchException("incorrect")
    }


  def outputWriter[T](fName: String, seq: Seq[T])(f: T => String): Unit = {
    val file = new File(fName)
    val bw = new BufferedWriter(new FileWriter(file))
    for (x <- seq) {
      bw.write(f(x))
    }
    bw.close()
  }

  def getUpvoteRatioByFavorites(postsFile: String, votesFile: String): Unit = {

    lazy val posts = SparkContextManager.sc.textFile(postsFile,
      minPartitions = 2)
      .map(recordParser(_, "post"))
      .filter(_.isInstanceOf[PostRecord])

    lazy val votes = SparkContextManager.sc.textFile(votesFile,
      minPartitions = 2)
      .map(recordParser(_, "vote"))
      .filter(_.isInstanceOf[VoteRecord])

    if (SparkContextManager.debug) {
      println(("#" * 80 + "\nTotal Posts: %s, Total Votes: %s")
        .format(posts.count(), votes.count()))
    }

    if (SparkContextManager.debug) {
      val voteSummary0 = votes.reduce(_ + _)
      println(("#" * 80 + "\nTotal UpVotes: %s, Total DownVotes: %s")
        .format(voteSummary0.ups, voteSummary0.downs))
    }


    val initialCount = (0, 0.0)
    val votesAndPosts =
      posts.map(p => (p.postID, p)).join(
        votes.map(v => (v.postID, v))
      )
    if (SparkContextManager.debug) {
      votesAndPosts take 10 foreach println
    }

    val outRecordRDD = votesAndPosts
      .map({ case (id: String, (post, vote)) =>
        (id, (post.favs, vote.ups, vote.downs))
      })

    if (SparkContextManager.debug) {
      println("#" * 80 + "\n outRecordRDD")
      outRecordRDD take 10 foreach println
    }

    val reducedOutRecordRDD = outRecordRDD
      .reduceByKey({ case (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3) })

    if (SparkContextManager.debug) {
      reducedOutRecordRDD take 10 foreach println
    }

    /*
    Computes the ratio of ups to downs, emits the favorites as key, and computes
    the average ratio for each amount of favorites
     */
    val ratioByFavoritesRDD = reducedOutRecordRDD
      .map({ case (_, (favs, ups, downs)) =>
        (favs, (ups.toDouble / (ups + downs).toDouble, 1))
      })
      .reduceByKey({ case ((rat1, count1), (rat2, count2)) =>
        (rat1 + rat2, count1 + count2)
      })
      .mapValues(t => t._1 / t._2.toDouble)

    val sortedRatioByFavorites = ratioByFavoritesRDD takeOrdered 50

    outputWriter("tmp/upvoteRatioByFavorites.csv",
      sortedRatioByFavorites)({
      case (k: Int, v: Double) => k.toString + "," + v.toString + ",\n"
    })

  }

  def tagParser(line: String): Seq[String] = {
    def parse = {
      val elem = XML.loadString(line)
      val tags: Seq[String] = (elem \ "@Tags" text) split "><"
      val out = tags map (_ stripPrefix "<" stripSuffix ">")
      out
    }
    Try(parse) getOrElse Seq()
  }

  def synonyms(postsFile: String): Unit = {

    lazy val tagsRDD = SparkContextManager.sc.textFile(postsFile,
      minPartitions = 2)
      .map(tagParser)
      .filter(_ != Seq())
      .map(Tuple1.apply)

    if (SparkContextManager.debug) {
      tagsRDD take 10 foreach println
    }
  }

  object SparkContextManager {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val debug = true

    def setMaster(master: String) = conf.setMaster(master)

    def stopSparkContext() = sc.stop()
  }

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(
        "Usage: SimpleApp <master> <postsFile> <votesFile> <usersFile>")
      System.exit(1)
    }
    val postsFile = args(1)
    val votesFile = args(2)
    val usersFile = args(3)

    SparkContextManager.setMaster(args(0))


    // getUpvoteRatioByFavorites(postsFile, votesFile)

    synonyms(postsFile)

    SparkContextManager.stopSparkContext()
  }
}
