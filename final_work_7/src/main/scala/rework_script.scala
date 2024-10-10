object StringProcessor {
    // Используем filter для фильтрации строк и map для преобразования вместо изменяемой переменной result
    def processStrings(strings: List[String]): List[String] = {
      // filter отбирает строки длиной более 3 символов
      // map преобразует оставшиеся строки в верхний регистр
      strings.filter(_.length > 3).map(_.toUpperCase)
    }
  
    def main(args: Array[String]): Unit = {
      val strings = List("apple", "cat", "banana", "dog", "elephant")
      val processedStrings = processStrings(strings)
      println(s"Processed strings: $processedStrings")
    }
  }
  