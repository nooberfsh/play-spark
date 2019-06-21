import org.apache.spark.sql.{
  Dataset,
  Encoder,
  KeyValueGroupedDataset,
  SparkSession
}
import org.apache.spark.sql.expressions.Aggregator
import shapeless.syntax.std.tuple._

object Implicits {
  implicit def kv2[K1, K2](d: Dataset[(K1, K2)]): ToCaseClass2[K1, K2] =
    new ToCaseClass2(d)
  implicit def kv3[K1, K2, K3](
      d: Dataset[(K1, K2, K3)]): ToCaseClass3[K1, K2, K3] = new ToCaseClass3(d)
  implicit def kv4[K1, K2, K3, K4](
      d: Dataset[(K1, K2, K3, K4)]): ToCaseClass4[K1, K2, K3, K4] =
    new ToCaseClass4(d)

  implicit def makeGroup[K, V](d: KeyValueGroupedDataset[K, V]) =
    new GroupedDataset(d)

  implicit def makeGroup2[K1, K2, V](d: KeyValueGroupedDataset[(K1, K2), V]) =
    new GroupedDataset2(d)
}

object Main extends App {
  val spark = SparkSession
    .builder()
    .master("local")
    .getOrCreate()

  import spark.implicits._
  import TypedFunctions._
  import Implicits._

  case class Bill(uid: Long, product: String, month: Long, money: Long)
  case class SumedBill(uid: Long, product: String, money: Long, count: Long)

  val bills = Seq(
    Bill(1, "p1", 1, 10),
    Bill(1, "p1", 1, 11),
    Bill(2, "p1", 1, 11)
  ).toDS

  val g =
    bills
      .groupByKey(r => (r.uid, r.product))
      .tagg(avg(r => r.money))
      .map(_ :+ 0L) to SumedBill.apply

  g.show()
}

class ToCaseClass2[T1, T2](g: Dataset[(T1, T2)]) {
  def to[B: Encoder](f: (T1, T2) => B): Dataset[B] = g.map {
    case (a, b) => f(a, b)
  }
}

class ToCaseClass3[T1, T2, T3](g: Dataset[(T1, T2, T3)]) {
  def to[B: Encoder](f: (T1, T2, T3) => B): Dataset[B] = g.map {
    case (a, b, c) => f(a, b, c)
  }
}

class ToCaseClass4[T1, T2, T3, T4](g: Dataset[(T1, T2, T3, T4)]) {
  def to[B: Encoder](f: (T1, T2, T3, T4) => B): Dataset[B] = g.map {
    case (a, b, c, d) => f(a, b, c, d)
  }
}

class GroupedDataset[K, V](g: KeyValueGroupedDataset[K, V]) {
  def tagg[U1](agg1: Aggregator[V, _, U1]): Dataset[(K, U1)] = {
    g.agg(agg1.toColumn)
  }

  def tagg[U1, U2](agg1: Aggregator[V, _, U1],
                   agg2: Aggregator[V, _, U2]): Dataset[(K, U1, U2)] = {
    g.agg(agg1.toColumn, agg2.toColumn)
  }

  def tagg[U1, U2, U3](agg1: Aggregator[V, _, U1],
                       agg2: Aggregator[V, _, U2],
                       agg3: Aggregator[V, _, U3]): Dataset[(K, U1, U2, U3)] = {
    g.agg(agg1.toColumn, agg2.toColumn, agg3.toColumn)
  }

  def tagg[U1, U2, U3, U4](
      agg1: Aggregator[V, _, U1],
      agg2: Aggregator[V, _, U2],
      agg3: Aggregator[V, _, U3],
      agg4: Aggregator[V, _, U4]): Dataset[(K, U1, U2, U3, U4)] = {
    g.agg(agg1.toColumn, agg2.toColumn, agg3.toColumn, agg4.toColumn)
  }
}

class GroupedDataset2[K1, K2, V](g: KeyValueGroupedDataset[(K1, K2), V]) {
  def tagg[U1](agg1: Aggregator[V, _, U1])(
      implicit e: Encoder[(K1, K2, U1)]): Dataset[(K1, K2, U1)] = {
    g.agg(agg1.toColumn).map { case ((k1, k2), v) => (k1, k2, v) }
  }

  def tagg[U1, U2](agg1: Aggregator[V, _, U1], agg2: Aggregator[V, _, U2])(
      implicit e: Encoder[(K1, K2, U1, U2)]): Dataset[(K1, K2, U1, U2)] = {
    g.agg(agg1.toColumn, agg2.toColumn).map {
      case ((k1, k2), v1, v2) => (k1, k2, v1, v2)
    }
  }

  def tagg[U1, U2, U3](
      agg1: Aggregator[V, _, U1],
      agg2: Aggregator[V, _, U2],
      agg3: Aggregator[V, _, U3])(implicit e: Encoder[(K1, K2, U1, U2, U3)])
    : Dataset[(K1, K2, U1, U2, U3)] = {
    g.agg(agg1.toColumn, agg2.toColumn, agg3.toColumn).map {
      case ((k1, k2), v1, v2, v3) => (k1, k2, v1, v2, v3)
    }
  }

  def tagg[U1, U2, U3, U4](
      agg1: Aggregator[V, _, U1],
      agg2: Aggregator[V, _, U2],
      agg3: Aggregator[V, _, U3],
      agg4: Aggregator[V, _, U4])(implicit e: Encoder[(K1, K2, U1, U2, U3, U4)])
    : Dataset[(K1, K2, U1, U2, U3, U4)] = {
    g.agg(agg1.toColumn, agg2.toColumn, agg3.toColumn, agg4.toColumn).map {
      case ((k1, k2), v1, v2, v3, v4) => (k1, k2, v1, v2, v3, v4)
    }
  }
}

object TypedFunctions {
  def sum[T, R](f: T => R)(implicit r: math.Numeric[R], encoder: Encoder[R]) =
    new Aggregator[T, R, R] {
      def zero: R = r.zero
      def reduce(b: R, a: T): R = r.plus(b, f(a))
      def merge(b1: R, b2: R): R = r.plus(b1, b2)
      def finish(r: R): R = r
      override def outputEncoder: Encoder[R] = encoder
      override def bufferEncoder: Encoder[R] = encoder
    }

  def count[T](implicit imp: Encoder[Long]) =
    new Aggregator[T, Long, Long] {
      def zero: Long = 0
      def reduce(b: Long, a: T): Long = b + 1
      def merge(b1: Long, b2: Long): Long = b1 + b2
      def finish(r: Long): Long = r
      override def outputEncoder: Encoder[Long] = imp
      override def bufferEncoder: Encoder[Long] = imp
    }

  def countDistinct[T, C](f: T => C)(implicit output: Encoder[Long],
                                     buffer: Encoder[Seq[Int]]) =
    new Aggregator[T, Seq[Int], Long] {
      def zero: Seq[Int] = Seq()
      def reduce(b: Seq[Int], a: T): Seq[Int] = {
        val hash = f(a).hashCode()
        if (b.contains(hash)) b else b :+ hash
      }
      def merge(b1: Seq[Int], b2: Seq[Int]): Seq[Int] = (b1 ++ b2).distinct
      def finish(r: Seq[Int]): Long = r.length.toLong
      override def outputEncoder: Encoder[Long] = output
      override def bufferEncoder: Encoder[Seq[Int]] = buffer
    }

  def avg[T, R](f: T => R)(implicit r: math.Numeric[R],
                           output: Encoder[R],
                           buffer: Encoder[(R, Long)]) =
    new Aggregator[T, (R, Long), R] {
      def zero: (R, Long) = (r.zero, 0)
      def reduce(b: (R, Long), a: T): (R, Long) = (r.plus(b._1, f(a)), b._2 + 1)
      def merge(b1: (R, Long), b2: (R, Long)): (R, Long) =
        (r.plus(b1._1, b2._1), b1._2 + b2._2)
      def finish(f: (R, Long)): R = {
        val (sum, num) = f
        if (num == 0) {
          r.zero
        } else
          // FIXME: 添加其他可能的类型
          sum match {
            case a: Short  => (a / num).toInt.asInstanceOf[R]
            case a: Int    => (a / num).toInt.asInstanceOf[R]
            case a: Long   => (a / num).asInstanceOf[R]
            case a: Float  => (a / num).asInstanceOf[R]
            case a: Double => (a / num).asInstanceOf[R]
          }

      }
      override def outputEncoder: Encoder[R] = output
      override def bufferEncoder: Encoder[(R, Long)] = buffer
    }
}
