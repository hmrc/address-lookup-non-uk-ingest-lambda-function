package object util extends Logging {
  def timed[T](block: => T): T = {
    val start = System.currentTimeMillis()
    val result = block
    val end = System.currentTimeMillis()

    logger.info(s""">>> Time take to run block: ${end - start}ms""")

    result
  }
}
