/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._

import scala.reflect.ClassTag
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
    def + (that: Record): Record = (this, that) match {
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

  object PostRecord extends Record {
    def parseXML(line: String): (String, Int) = {
      val elem = XML.loadString(line)
      val postID: String = elem \ "@Id" text
      val favs: Int = (elem \ "@FavoriteCount" text).toInt
      val out = (postID, favs)
      out
    }
  }
  case class PostRecord private (override val postID: String,
                         override val favs: Int) extends Record {
    def this(t: (String, Int)) = this(t._1, t._2)
    def this(line: String) = this(PostRecord.parseXML(line))
  }

  object VoteRecord extends Record {

    def parseVoteXML(line: String): (String, Int, Int) = {
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
    }
  }

  case class VoteRecord (override val postID: String,
                         override val ups: Int,
                         override val downs: Int) extends Record {
    def this(t: (String, Int, Int)) = this(t._1, t._2, t._3)
    def this(line: String) = this(VoteRecord.parseVoteXML(line))

  }

  case class UserRecord (override val userID: String,
                         override val rep: Int) extends Record

  case class OutRecord(override val favs: Int,
                       override val ups: Int,
                       override val downs: Int) extends Record {
    /**
      * Auxilliary constructor
      *
      * @param pRec a PostRecord instance
      * @param vRec a VoteRecord instance
      * @return a new OutRecord instance
      */
    def this(pRec: PostRecord, vRec: VoteRecord) =
      this(pRec.favs, vRec.ups, vRec.downs)
  }

  /*
    * Parses a line of a post XML to generate a post record
    *
    * @param line the string corresponding to a single input line
    * @return a PostRecord instance
    */
  def postParser(line: String): Record =
    Try(new PostRecord(line)) getOrElse EmptyRecord


  /**
    * Parses a line of a vote XML to generate a vote record
    *
    * @param line the string corresponding to a single input line
    * @return a VoteRecord instance
    */
  def voteParser(line: String): Record =
    Try(new VoteRecord(line)) getOrElse EmptyRecord

  /*
  TODO: figure out how to make this polymorphic for classes
  def recordParser[T: ClassTag](line: String): Record =
    Try(new ClassTag[T](line)) getOrElse EmptyRecord
  */

  /**
    * Parses a line of a user XML to generate a user record
    *
    * @param line the string corresponding to a single input line
    * @return a UserRecord instance
    */
  def userParser(line: String): UserRecord = {
    val idPattern = "Id=\"(\\-*\\d+)\"".r
    val repPattern = "Reputation=\"(\\-*\\d+)\"".r
    def loop(xs: List[String], id: String, rep: Int): UserRecord = xs match {
      case List() => new UserRecord(id, rep)
      case (x :: xs1) => x match {
        case idPattern(new_id) => loop(xs1, new_id, rep)
        case repPattern(new_rep) => loop(xs1, id, new_rep.toInt)
        case _ => loop(xs1, id, rep)
      }
    }
    loop(line.split("\\s+").toList, "", 0)
  }


  def outputWriter[T](fName: String, seq: Seq[T])(f: T => String): Unit = {
    val file = new File(fName)
    val bw = new BufferedWriter(new FileWriter(file))
    for (x <- seq) {
      bw.write(f(x))
    }
    bw.close()
  }


  object SparkContextManager {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val debug = true
  }


  def getUpvotePctByFavorites(postsFile: String, votesFile: String): Unit = {

    lazy val posts = SparkContextManager.sc.textFile(postsFile,
      minPartitions = 2)
      .filter(_.contains("<row "))
      .map(postParser(_))
      .filter(_.isInstanceOf[PostRecord])

    lazy val votes = SparkContextManager.sc.textFile(votesFile,
      minPartitions = 2)
      .filter(_.contains("<row "))
      .map(voteParser(_))
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


    val initialCount = (0,0.0)
    val votesAndPosts =
      posts.map(p => (p.postID, p)).join(
        votes.map(v => (v.postID, v))
      )

    votesAndPosts take 10 foreach println
    /*
    val outRecordRDD = votesAndPosts
      .map({
        case (id: String, (post, vote)) =>
          (id, new OutRecord(post.favs, vote.ups, vote.downs))
      })
    outRecordRDD take 10 foreach println
        .map({
          case (id: String, (post, vote)) =>
            (id, new OutRecord(post.favs, vote.ups, vote.downs))
        })
        .filter({ case (_: String, r: OutRecord) => r.ups > 0 | r.downs > 0 })
        .reduceByKey(_ + _)
        .map({ case (t: String, r: OutRecord) =>
          (r.favs, (r.ups.toDouble / (r.ups + r.downs).toDouble, 1))
        })
        .reduceByKey({ case ((value1, count1), (value2, count2)) =>
          (value1 + value2, count1 + count2)
        })
        .mapValues({ case (values, counts) => values / counts.toDouble })

    if (SparkContextManager.debug) {
      votesByPost.saveAsTextFile("tmp/votesByPost")
    }

    val upvotePctByFavorites = votesByPost takeOrdered 50

    outputWriter("tmp/upvotePctByFavorites.csv",
      upvotePctByFavorites)({
      case (k: Int, v: Double) => k.toString + "," + v.toString + ",\n"
    })
    */
  }


  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Requires exactly two inputs")
      System.exit(1)
    }

    val postsFile = args(0)
    val votesFile = args(1)

    getUpvotePctByFavorites(postsFile, votesFile)

    SparkContextManager.sc.stop()
  }
}
