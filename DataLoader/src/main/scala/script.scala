package scala
class Fruit{}
case class Apple(words:String) extends Fruit
case class Banana(words:String) extends Fruit

object script {
  def main(args: Array[String]): Unit = {
    var apple: Fruit = new Apple("apple is sweet")
    apple match {
      case Apple(words)=>println(words)
      case Banana(words)=>println("banana is supper "+words)
      case _ => println("i dont know")
    }

    println(apple.isInstanceOf[Fruit]) // true
    println(apple.isInstanceOf[Apple]) // true
    println(apple.isInstanceOf[Banana]) // false
  }

}
