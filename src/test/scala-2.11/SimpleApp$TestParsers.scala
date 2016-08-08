import SimpleApp.{PostRecord, VoteRecord, UserRecord, tagParser}
import org.scalatest.FunSuite

/**
  * Created by joshuaarnold on 8/5/16.
  */
class SimpleApp$TestParsers extends FunSuite {

  test("testPostParser") {
    val postLine =
      """<row Id="5" PostTypeId="1"
        |CreationDate="2014-05-13T23:58:30.457" Score="7" ViewCount="296"
        |Body="&lt;p&gt;I've always been interested in machine
        |learning, but I can't figure out one thing about starting out with a
        |simple &quot;Hello World&quot; example - how can I avoid hard-coding
        |behavior     ?&lt;/p&gt;&#xA;&#xA;&lt;p&gt;For example, if I wanted to
        |&quot;teach&quot; a bot how to avoid randomly placed obstacles, I
        |couldn't just use relat     ive motion, because the obstacles move
        |around, but I don't want to hard code, say, distance, because that
        |ruins the whole point of machine learning
        |.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;Obviously, randomly generating code
        |would
        |be impractical, so how could I do this?&lt;/p&gt;&#xA;"
        |OwnerUserId="5" LastActivityDate="2014-05-14T00:36:31.077"
        |Title="How can I do simple machine learning without hard-coding
        |behavior?" Tags="&lt;machine-learning&gt;"      AnswerCount="1"
        |CommentCount="1" FavoriteCount="1" ClosedDate="2014-05-14T14:40:25.950"
        | />""".stripMargin

    assert(new PostRecord(postLine) ===
      new PostRecord("5", 1))
  }

  test("testVoteParser") {
    val voteLine =
      """<row Id="1" PostId="1" VoteTypeId="2"
        |CreationDate="2014-05-13T00:00:00.000" />""".stripMargin

    assert(new VoteRecord(voteLine) === new VoteRecord("1", 1, 0))
  }

  test("testUserParser") {
    val userLine =
      """ <row Id="-1" Reputation="1" CreationDate="2014-05-13T21:29:22.820"
        |DisplayName="Community" LastAccessDate="2014-05-13T21:29:22.820"
        |WebsiteUrl="http://meta.stackexchange.com/"
        |Location="on the server farm" AboutMe="&lt;p&gt;Hi, I'm not
        |really a person.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;I'm a background process
        |that helps keep this site clean!&lt;/p&gt;&#xA;&#xA;&lt;p&gt;I
        |do things like&lt;/p&gt;&#xA;&#xA;&lt;ul&gt;&#xA;&lt;li&gt;Randomly
        |poke
        |old unanswered questions every hour so they get some
        |attention&lt;/li&gt;&#xA;&lt;li&gt;Own community questions and
        |answers so nobody gets
        |unnecessary reputation from them&lt;/li&gt;&#xA;&lt;li&gt;Own downvotes
        | on spam/evi      l posts that get permanently
        | deleted&lt;/li&gt;&#xA;&lt;li&gt;Own suggeste      d edits from
        | anonymous users&lt;/li&gt;&#xA;&lt;li&gt;&lt;a href=&quot;htt
        | p://meta.stackexchange.com/a/92006&quot;&gt;Remove abandoned
        | questions&lt;      /a&gt;&lt;/li&gt;&#xA;&lt;/ul&gt;&#xA;"
        | Views="0" UpVotes="749" DownVotes=      "221" AccountId="-1"
        | /> """.stripMargin

    assert(UserRecord(userLine) === new UserRecord("-1", 1))
  }


}
