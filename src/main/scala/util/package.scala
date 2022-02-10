package object util {
  def timed[T](block: => T): T = {
    val start = System.currentTimeMillis()
    val result = block
    val end = System.currentTimeMillis()

    println(s""">>> Time take to run block: ${end - start}ms""")

    result
  }
}
