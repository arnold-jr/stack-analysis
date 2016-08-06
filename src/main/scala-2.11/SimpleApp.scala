/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._

object SimpleApp {

  abstract class Record
  case class EmptyRecord() extends Record
  case class PostRecord (id: String, favs: Int) extends Record
  case class VoteRecord (postid: String, ups: Int, downs: Int) extends Record {
    /**
      * Defines addition operation to sum upvotes and downvotes
      *
      * @param that VoteRecord to add to this VoteRecord
      * @return new VoteRecord with sum of ups and downs
      */
    def + (that: VoteRecord) =
      new VoteRecord(this.postid,
        this.ups + that.ups,
        this.downs + that.downs)

    override def toString = "PostId: " + postid + " " + ups + "/" + downs
  }
  case class UserRecord (userid: String, rep: Int) extends Record
  case class OutRecord(favs: Int, ups: Int, downs: Int) extends Record {
    /**
      * Auxilliary constructor
      *
      * @param pRec a PostRecord instance
      * @param vRec a VoteRecord instance
      * @return a new OutRecord instance
      */
    def this(pRec: PostRecord, vRec: VoteRecord) =
      this(pRec.favs, vRec.ups, vRec.downs)

    /**
      * Defines addition operation to sum favorites, upvotes, and downvotes
      *
      * @param that OutRecord to add to this OutRecord
      * @return new OutRecord with sum of favs, ups, and downs
      */
    def + (that: OutRecord) =
      new OutRecord(this.favs + that.favs,
        this.ups + that.ups,
        this.downs + that.downs)
  }


  /**
    * Parses a line of a post XML to generate a post record
    *
    * @param line the string corresponding to a single input line
    * @return a PostRecord instance
    */
  def postParser(line: String): PostRecord = {
    val idPattern = "Id=\"(\\d+)\"".r
    val favsPattern = "FavoriteCount=\"(\\d+)\"".r
    def loop(xs: List[String], id: String, favs: Int): PostRecord = xs match {
      case List() => new PostRecord(id, favs)
      case (x :: xs1) => x match {
        case idPattern(newId) => loop(xs1, newId, favs)
        case favsPattern(newFavs) => loop(xs1, id, newFavs.toInt)
        case _ => loop(xs1, id, favs)
      }
    }
    loop(line.split("\\s+").toList, "", 0)
  }


  /**
    * Parses a line of a vote XML to generate a vote record
    *
    * @param line the string corresponding to a single input line
    * @return a VoteRecord instance
    */
  def voteParser(line: String): VoteRecord = {
    val idPattern = "PostId=\"(\\d+)\"".r
    val vidPattern = "VoteTypeId=\"(\\d+)\"".r
    def loop(xs: List[String], id: String, up: Int, down: Int):
    VoteRecord = xs match {
      case List() => new VoteRecord(id, up, down)
      case (x :: xs1) => x match {
        case idPattern(new_id) => loop(xs1, new_id, up, down)
        case vidPattern(vid) =>
          if (vid == "2") {
            loop(xs1, id, 1, 0)
          } else if (vid == "3") {
            loop(xs1, id, 0, 1)
          } else {
            loop(xs1, id, 0, 0)
          }
        case _ => loop(xs1, id, up, down)
      }
    }
    loop(line.split("\\s+").toList,"",0,0)
  }


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
  }

  def routine1(postsFile: String, votesFile: String): Unit = {

    val debug = false

    lazy val posts = SparkContextManager.sc.textFile(postsFile,
      minPartitions = 2)
      .filter(_.contains("<row "))
      .map(postParser(_))

    lazy val votes = SparkContextManager.sc.textFile(votesFile,
      minPartitions = 2)
      .filter(_.contains("<row "))
      .map(voteParser(_))

    if (debug) {
      println(("#" * 80 + "\nTotal Posts: %s, Total Votes: %s")
        .format(posts.count(), votes.count()))
    }

    if (debug) {
      val voteSummary0 = votes.reduce(_ + _)
      println(("#" * 80 + "\nTotal UpVotes: %s, Total DownVotes: %s")
        .format(voteSummary0.ups, voteSummary0.downs))
    }

    val initialCount = (0,0.0)
    val votesByPost =
      posts.map(p => (p.id, p)).join(
        votes.map(v => (v.postid, v))
      )
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

    if (debug) {
      votesByPost.saveAsTextFile("tmp/votesByPost")
    }

    val upvotePctByFavorites = votesByPost takeOrdered 50

    outputWriter("tmp/upvotePctByFavorites.csv",
      upvotePctByFavorites)({
      case (k: Int, v: Double) => k.toString + "," + v.toString + ",\n"
    })

  }
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Requires exactly two inputs")
      System.exit(1)
    }
    val debug = false

    val postsFile = args(0)
    val votesFile = args(1)

    routine1(postsFile, votesFile)

    SparkContextManager.sc.stop()
  }
}
