

class MyString(val jString: String) {
  private var extraData = ""

  override def toString = jString + extraData
}

object MyString {
  def apply(base: String, extras: String) = {
    val s = new MyString(base)
    s.extraData = extras
    s
  }
}

println(new MyString())

