package org.ace

import MayErr._
import java.util.Date

abstract class CypherRequestError
case class ColumnNotFound(columnName: String, possibilities: List[String]) extends CypherRequestError {
  override def toString = columnName + " not found, available columns : " + possibilities.map { p => p.dropWhile(c => c == '.') }
    .mkString(", ")
}

case class TypeDoesNotMatch(message: String) extends CypherRequestError
case class UnexpectedNullableFound(on: String) extends CypherRequestError
case object NoColumnsInReturnedResult extends CypherRequestError
case class CypherMappingError(msg: String) extends CypherRequestError

trait Column[A] extends ((Any, MetaDataItem) => MayErr[CypherRequestError, A])

object Column {

  def apply[A](transformer: ((Any, MetaDataItem) => MayErr[CypherRequestError, A])): Column[A] = new Column[A] {
    def apply(value: Any, meta: MetaDataItem): MayErr[CypherRequestError, A] = transformer(value, meta)
  }

  def nonNull[A](transformer: ((Any, MetaDataItem) => MayErr[CypherRequestError, A])): Column[A] = Column[A] {
    case (value, meta @ MetaDataItem(column, _, _)) =>
      if (value != null) transformer(value, meta) else Left(UnexpectedNullableFound(column.toString))
  }

  implicit def rowToBoolean: Column[Boolean] = Column.nonNull { (value, meta) =>
    val MetaDataItem(column, nullable, clazz) = meta
    value match {
      case bool: Boolean => Right(bool)
      case _ => Left(TypeDoesNotMatch("Cannot convert " + value + ":" + value.asInstanceOf[AnyRef].getClass + " to Boolean for column " + column))
    }
  }

  implicit def rowToByte: Column[Byte] = Column.nonNull { (value, meta) =>
    val MetaDataItem(column, nullable, clazz) = meta
    value match {
      case byte: Byte => Right(byte)
      case _ => Left(TypeDoesNotMatch("Cannot convert " + value + ":" + value.asInstanceOf[AnyRef].getClass + " to Byte for column " + column))
    }
  }

  implicit def rowToShort: Column[Short] = Column.nonNull { (value, meta) =>
    val MetaDataItem(column, nullable, clazz) = meta
    value match {
      case short: Short => Right(short)
      case _ => Left(TypeDoesNotMatch("Cannot convert " + value + ":" + value.asInstanceOf[AnyRef].getClass + " to Short for column " + column))
    }
  }

  implicit def rowToInt: Column[Int] = Column.nonNull { (value, meta) =>
    val MetaDataItem(column, nullable, clazz) = meta
    value match {
      case int: Int => Right(int)
      case _ => Left(TypeDoesNotMatch("Cannot convert " + value + ":" + value.asInstanceOf[AnyRef].getClass + " to Int for column " + column))
    }
  }

  implicit def rowToLong: Column[Long] = Column.nonNull { (value, meta) =>
    val MetaDataItem(column, nullable, clazz) = meta
    value match {
      case int: Int => Right(int.toLong)
      case long: Long => Right(long)
      case _ => Left(TypeDoesNotMatch("Cannot convert " + value + ":" + value.asInstanceOf[AnyRef].getClass + " to Long for column " + column))
    }
  }

  implicit def rowToFloat: Column[Float] = Column.nonNull { (value, meta) =>
    val MetaDataItem(column, nullable, clazz) = meta
    value match {
      case f: Float => Right(f)
      case _ => Left(TypeDoesNotMatch("Cannot convert " + value + ":" + value.asInstanceOf[AnyRef].getClass + " to Float for column " + column))
    }
  }

  implicit def rowToDouble: Column[Double] = Column.nonNull { (value, meta) =>
    val MetaDataItem(column, nullable, clazz) = meta
    value match {
      case f: Float => Right(f.toDouble)
      case d: Double => Right(d)
      case _ => Left(TypeDoesNotMatch("Cannot convert " + value + ":" + value.asInstanceOf[AnyRef].getClass + " to Double for column " + column))
    }
  }

  implicit def rowToChar: Column[Char] = Column.nonNull { (value, meta) =>
    val MetaDataItem(column, nullable, clazz) = meta
    value match {
      case c: Char => Right(c)
      case _ => Left(TypeDoesNotMatch("Cannot convert " + value + ":" + value.asInstanceOf[AnyRef].getClass + " to Char for column " + column))
    }
  }

  implicit def rowToString: Column[String] = {
    Column.nonNull[String] { (value, meta) =>
      val MetaDataItem(column, nullable, clazz) = meta
      value match {
        case string: String => Right(string)
        case _ => Left(TypeDoesNotMatch("Cannot convert " + value + ":" + value.asInstanceOf[AnyRef].getClass + " to String for column " + column))
      }
    }
  }

  implicit def rowToDate: Column[Date] = Column.nonNull { (value, meta) =>
    val MetaDataItem(column, nullable, clazz) = meta
    value match {
      case long: Long => Right(new Date(long))
      case _ => Left(TypeDoesNotMatch("Cannot convert " + value + ":" + value.asInstanceOf[AnyRef].getClass + " to Date for column " + column))
    }
  }

  implicit def rowToDbValue[T](implicit transformer: Column[T]): Column[DbValue[T]] = Column { case (value, meta) =>
    if (value != null) transformer(value, meta).map(Assigned(_)) else Right(Unassigned): MayErr[CypherRequestError, DbValue[T]]
  }

  implicit def rowToNode: Column[org.neo4j.graphdb.Node] = Column.nonNull { case (value, meta) =>
    val MetaDataItem(qualified, nullable, clazz) = meta
    value match {
      case node: org.neo4j.kernel.impl.core.NodeProxy => Right(node)
      case node: org.neo4j.graphdb.Node => Right(node)
      case _ => Left(TypeDoesNotMatch("Cannot convert " + value + ":" + value.asInstanceOf[AnyRef].getClass + " to Node for column " + qualified))
    }
  }

  implicit def rowToRelationship: Column[org.neo4j.graphdb.Relationship] = Column.nonNull { case (value, meta) =>
    val MetaDataItem(qualified, nullable, clazz) = meta
    value match {
      case rel: org.neo4j.kernel.impl.core.RelationshipProxy => Right(rel)
      case rel: org.neo4j.graphdb.Relationship => Right(rel)
      case _ => Left(TypeDoesNotMatch("Cannot convert " + value + ":" + value.asInstanceOf[AnyRef].getClass + " to Relationship for column " + qualified))
    }
  }

  implicit def rowToPath: Column[Seq[org.neo4j.graphdb.PropertyContainer]] = Column.nonNull { case (value, meta) =>
    val MetaDataItem(qualified, nullable, clazz) = meta
    value match {
      case path: org.neo4j.cypher.PathImpl => foldSeq(path.toSeq)(
        value => rowToNode(value, MetaDataItem(qualified, false, value.getClass().getName())),
        value => rowToRelationship(value, MetaDataItem(qualified, false, value.getClass().getName())))
      case _ => Left(TypeDoesNotMatch("Cannot convert " + value + ":" + value.asInstanceOf[AnyRef].getClass + " to Relationship for column " + qualified))
    }
  }

  implicit def rowToOption[T](implicit transformer: Column[T]): Column[Option[T]] = Column { (value, meta) =>
    if (value != null) transformer(value, meta).map(Some(_)) else (Right(None): MayErr[CypherRequestError, Option[T]])
  }

  implicit def rowToSeq[T](implicit transformer: Column[T]): Column[Seq[T]] = Column { (value, meta) =>
    val MetaDataItem(qualified, nullable, clazz) = meta
    value match {
      case seq: Seq[_] => foldSeq(seq)(
        value => transformer(value, MetaDataItem(qualified, false, value.getClass().getName())))
      case arr: Array[_] => foldSeq(arr)(
        value => transformer(value, MetaDataItem(qualified, false, value.getClass().getName())))
      case _ => Left(TypeDoesNotMatch("Cannot convert " + value + ":" + value.asInstanceOf[AnyRef].getClass + " to Seq for column " + qualified))
    }
  }

  private def foldSeq[T](seq: Seq[Any])(transformers: (Any => MayErr[CypherRequestError, T])*) = {
    assert(!transformers.isEmpty)
    seq.foldLeft(Right(Seq.empty): MayErr[CypherRequestError, Seq[T]]) {
      case (seq, value) => for {
        init <- seq
        last <- {
          val ts = transformers.map(f => f(value))
          ts.find(_.isRight).getOrElse(ts.head)
        }
      } yield init :+ last
    }
  }
}

case class TupleFlattener[F](f: F)

trait PriorityOne {
  implicit def flattenerTo2[T1, T2]: TupleFlattener[(T1 ~ T2) => (T1, T2)] = TupleFlattener[(T1 ~ T2) => (T1, T2)] { case (t1 ~ t2) => (t1, t2) }
}

trait PriorityTwo extends PriorityOne {
  implicit def flattenerTo3[T1, T2, T3]: TupleFlattener[(T1 ~ T2 ~ T3) => (T1, T2, T3)] = TupleFlattener[(T1 ~ T2 ~ T3) => (T1, T2, T3)] { case (t1 ~ t2 ~ t3) => (t1, t2, t3) }
}

trait PriorityThree extends PriorityTwo {
  implicit def flattenerTo4[T1, T2, T3, T4]: TupleFlattener[(T1 ~ T2 ~ T3 ~ T4) => (T1, T2, T3, T4)] = TupleFlattener[(T1 ~ T2 ~ T3 ~ T4) => (T1, T2, T3, T4)] { case (t1 ~ t2 ~ t3 ~ t4) => (t1, t2, t3, t4) }
}

trait PriorityFour extends PriorityThree {
  implicit def flattenerTo5[T1, T2, T3, T4, T5]: TupleFlattener[(T1 ~ T2 ~ T3 ~ T4 ~ T5) => (T1, T2, T3, T4, T5)] = TupleFlattener[(T1 ~ T2 ~ T3 ~ T4 ~ T5) => (T1, T2, T3, T4, T5)] { case (t1 ~ t2 ~ t3 ~ t4 ~ t5) => (t1, t2, t3, t4, t5) }
}

trait PriorityFive extends PriorityFour {
  implicit def flattenerTo6[T1, T2, T3, T4, T5, T6]: TupleFlattener[(T1 ~ T2 ~ T3 ~ T4 ~ T5 ~ T6) => (T1, T2, T3, T4, T5, T6)] = TupleFlattener[(T1 ~ T2 ~ T3 ~ T4 ~ T5 ~ T6) => (T1, T2, T3, T4, T5, T6)] { case (t1 ~ t2 ~ t3 ~ t4 ~ t5 ~ t6) => (t1, t2, t3, t4, t5, t6) }
}

trait PrioritySix extends PriorityFive {
  implicit def flattenerTo7[T1, T2, T3, T4, T5, T6, T7]: TupleFlattener[(T1 ~ T2 ~ T3 ~ T4 ~ T5 ~ T6 ~ T7) => (T1, T2, T3, T4, T5, T6, T7)] = TupleFlattener[(T1 ~ T2 ~ T3 ~ T4 ~ T5 ~ T6 ~ T7) => (T1, T2, T3, T4, T5, T6, T7)] { case (t1 ~ t2 ~ t3 ~ t4 ~ t5 ~ t6 ~ t7) => (t1, t2, t3, t4, t5, t6, t7) }
}

trait PrioritySeven extends PrioritySix {
  implicit def flattenerTo8[T1, T2, T3, T4, T5, T6, T7, T8]: TupleFlattener[(T1 ~ T2 ~ T3 ~ T4 ~ T5 ~ T6 ~ T7 ~ T8) => (T1, T2, T3, T4, T5, T6, T7, T8)] = TupleFlattener[(T1 ~ T2 ~ T3 ~ T4 ~ T5 ~ T6 ~ T7 ~ T8) => (T1, T2, T3, T4, T5, T6, T7, T8)] { case (t1 ~ t2 ~ t3 ~ t4 ~ t5 ~ t6 ~ t7 ~ t8) => (t1, t2, t3, t4, t5, t6, t7, t8) }
}

trait PriorityEight extends PrioritySeven {
  implicit def flattenerTo9[T1, T2, T3, T4, T5, T6, T7, T8, T9]: TupleFlattener[(T1 ~ T2 ~ T3 ~ T4 ~ T5 ~ T6 ~ T7 ~ T8 ~ T9) => (T1, T2, T3, T4, T5, T6, T7, T8, T9)] = TupleFlattener[(T1 ~ T2 ~ T3 ~ T4 ~ T5 ~ T6 ~ T7 ~ T8 ~ T9) => (T1, T2, T3, T4, T5, T6, T7, T8, T9)] { case (t1 ~ t2 ~ t3 ~ t4 ~ t5 ~ t6 ~ t7 ~ t8 ~ t9) => (t1, t2, t3, t4, t5, t6, t7, t8, t9) }
}

trait PriorityNine extends PriorityEight {
  implicit def flattenerTo10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]: TupleFlattener[(T1 ~ T2 ~ T3 ~ T4 ~ T5 ~ T6 ~ T7 ~ T8 ~ T9 ~ T10) => (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)] = TupleFlattener[(T1 ~ T2 ~ T3 ~ T4 ~ T5 ~ T6 ~ T7 ~ T8 ~ T9 ~ T10) => (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)] { case (t1 ~ t2 ~ t3 ~ t4 ~ t5 ~ t6 ~ t7 ~ t8 ~ t9 ~ t10) => (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10) }
}

object TupleFlattener extends PriorityNine {
  implicit def flattenerTo11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]: TupleFlattener[(T1 ~ T2 ~ T3 ~ T4 ~ T5 ~ T6 ~ T7 ~ T8 ~ T9 ~ T10 ~ T11) => (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)] = TupleFlattener[(T1 ~ T2 ~ T3 ~ T4 ~ T5 ~ T6 ~ T7 ~ T8 ~ T9 ~ T10 ~ T11) => (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)] { case (t1 ~ t2 ~ t3 ~ t4 ~ t5 ~ t6 ~ t7 ~ t8 ~ t9 ~ t10 ~ t11) => (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11) }
}

object Row {
  def unapplySeq(row: Row): Option[List[Any]] = Some(row.asList)
}

case class MetaDataItem(column: String, nullable: Boolean, clazz: String)

case class MetaData(ms: List[MetaDataItem]) {
  
  def get(columnName: String) = {
    val columnUpper = columnName.toUpperCase()
    dictionary.get(columnUpper)
  }

  private lazy val dictionary: Map[String, (String, Boolean, String)] =
    ms.map(m => (m.column.toUpperCase(), (m.column, m.nullable, m.clazz))).toMap

  lazy val columnCount = ms.size

  lazy val availableColumns: List[String] = ms.map(_.column)
}

trait Row {
  val metaData: MetaData

  protected[ace] val data: List[Any]

  lazy val asList = data.zip(metaData.ms.map(_.nullable)).map(i => if (i._2) Option(i._1) else i._1)

  lazy val asMap: scala.collection.Map[String, Any] = metaData.ms.map(_.column).zip(asList).toMap

  def get[A](a: String)(implicit c: Column[A]): MayErr[CypherRequestError, A] = CypherParser.get(a)(c)(this) match {
    case Success(a) => Right(a)
    case Error(e) => Left(e)
  }

  private def getType(t: String) = t match {
    case "long" => Class.forName("java.lang.Long")
    case "int" => Class.forName("java.lang.Integer")
    case "boolean" => Class.forName("java.lang.Boolean")
    case _ => Class.forName(t)
  }

  private lazy val ColumnsDictionary: Map[String, Any] = metaData.ms.map(_.column.toUpperCase()).zip(data).toMap
  private[ace] def get1(a: String): MayErr[CypherRequestError, Any] = {
    for (
      meta <- metaData.get(a).toRight(ColumnNotFound(a, metaData.availableColumns));
      (column, nullable, clazz) = meta;
      result <- ColumnsDictionary.get(column.toUpperCase()).toRight(ColumnNotFound(column, metaData.availableColumns))
    ) yield result
  }

  def apply[B](a: String)(implicit c: Column[B]): B = get[B](a)(c).get
}

case class MockRow(data: List[Any], metaData: MetaData) extends Row

case class CypherRow(metaData: MetaData, data: List[Any]) extends Row {
  override def toString() = "Row(" + metaData.ms.zip(data).map(t => "'" + t._1.column + "':" + t._2 + " as " + t._1.clazz).mkString(", ") + ")"
}

object Useful {

  case class Var[T](var content: T)

  def drop[A](these: Var[Stream[A]], n: Int): Stream[A] = {
    var count = n
    while (!these.content.isEmpty && count > 0) {
      these.content = these.content.tail
      count -= 1
    }
    these.content
  }

  def unfold1[T, R](init: T)(f: T => Option[(R, T)]): (Stream[R], T) = f(init) match {
    case None => (Stream.Empty, init)
    case Some((r, v)) => (Stream.cons(r, unfold(v)(f)), v)
  }

  def unfold[T, R](init: T)(f: T => Option[(R, T)]): Stream[R] = f(init) match {
    case None => Stream.Empty
    case Some((r, v)) => Stream.cons(r, unfold(v)(f))
  }
}

case class SimpleCypher[T](query: CypherQuery, params: Map[String, Any] = Map(), defaultParser: RowParser[T]) extends Cypher {

  def on(args: (String, Any)*) = this.copy(params = (this.params) ++ args)

  def list()(implicit service: org.neo4j.graphdb.GraphDatabaseService): Seq[T] = as(defaultParser*)

  def single()(implicit service: org.neo4j.graphdb.GraphDatabaseService): T = as(ResultSetParser.single(defaultParser))

  def singleOpt()(implicit service: org.neo4j.graphdb.GraphDatabaseService): Option[T] = as(ResultSetParser.singleOpt(defaultParser))

  def getExecutionResult()(implicit service: org.neo4j.graphdb.GraphDatabaseService): org.neo4j.cypher.ExecutionResult =
    Cypher.getExecutionResult(query.query, params ++ query.params)

  def using[U](p: RowParser[U]): SimpleCypher[U] = SimpleCypher(query, params, p)
}

trait Cypher {

  protected def getExecutionResult()(implicit service: org.neo4j.graphdb.GraphDatabaseService): org.neo4j.cypher.ExecutionResult

  def apply()(implicit service: org.neo4j.graphdb.GraphDatabaseService) = Cypher.executionResultToStream(executionResult())

  def executionResult()(implicit service: org.neo4j.graphdb.GraphDatabaseService) = getExecutionResult()

  def as[T](parser: ResultSetParser[T])(implicit service: org.neo4j.graphdb.GraphDatabaseService): T = Cypher.as[T](parser, executionResult())

  def list[A](rowParser: RowParser[A])(implicit service: org.neo4j.graphdb.GraphDatabaseService): Seq[A] = as(rowParser *)

  def single[A](rowParser: RowParser[A])(implicit service: org.neo4j.graphdb.GraphDatabaseService): A = as(ResultSetParser.single(rowParser))

  def singleOpt[A](rowParser: RowParser[A])(implicit service: org.neo4j.graphdb.GraphDatabaseService): Option[A] = as(ResultSetParser.singleOpt(rowParser))

  def parse[T](parser: ResultSetParser[T])(implicit service: org.neo4j.graphdb.GraphDatabaseService): T = Cypher.parse[T](parser, executionResult())

  def execute()(implicit service: org.neo4j.graphdb.GraphDatabaseService): Boolean =
    scala.util.control.Exception.allCatch[org.neo4j.cypher.ExecutionResult]
      .opt(getExecutionResult()).isDefined

  type Statistic = (Int, Int, Int, Int, Int)
  def executeUpdate()(implicit service: org.neo4j.graphdb.GraphDatabaseService): Statistic = {
    val qs = getExecutionResult().queryStatistics()
    (qs.nodesCreated.toInt,
     qs.relationshipsCreated.toInt,
     qs.propertiesSet.toInt,
     qs.deletedNodes.toInt,
     qs.deletedRelationships.toInt)
  }
}

case class CypherQuery(query: String, params: Map[String, Any] = Map()) extends Cypher {

  def getExecutionResult()(implicit service: org.neo4j.graphdb.GraphDatabaseService): org.neo4j.cypher.ExecutionResult =
    Cypher.getExecutionResult(query, params)

  private def defaultParser: RowParser[Row] = RowParser(row => Success(row))

  def asSimple: SimpleCypher[Row] = SimpleCypher(this, Map.empty, defaultParser)

  def asSimple[T](parser: RowParser[T] = defaultParser): SimpleCypher[T] = SimpleCypher(this, Map.empty, parser)
}

object Cypher {

  private[ace] def getExecutionResult(query: String, params: Map[String, Any])(implicit service: org.neo4j.graphdb.GraphDatabaseService): org.neo4j.cypher.ExecutionResult = {
    val ee = new org.neo4j.cypher.ExecutionEngine(service)
    ee.execute(query, params)
  }

  def apply(query: String): CypherQuery = CypherQuery(query)

  def executionResultToStream(er: org.neo4j.cypher.ExecutionResult): Stream[CypherRow] = {
    val columns = er.columns
    val metaDict = scala.collection.mutable.HashMap[String, String]()
    val dataStrm = Useful.unfold(er) { er =>
      if(er.hasNext) {
        val row = er.next()
        val res = for(col <- columns) yield {
          val cell = row(col)
          if(cell != null) {
            val clazz = cell.getClass().getName()
            for(prev <- metaDict.put(col, clazz)) {
              assert(prev == clazz)
            }
          }
          cell
        }
        Some(res, er)
      } else {
        None
      }
    }
    val metaData = MetaData(columns.map(name => MetaDataItem(name, true, metaDict.getOrElse(name, "java.lang.Object"))))
    dataStrm.map(data => CypherRow(metaData, data))
  }

  def as[T](parser: ResultSetParser[T], er: org.neo4j.cypher.ExecutionResult): T =
    parser(executionResultToStream(er)) match {
      case Success(a) => a
      case Error(e) => sys.error(e.toString)
    }

  def parse[T](parser: ResultSetParser[T], er: org.neo4j.cypher.ExecutionResult): T =
    parser(executionResultToStream(er)) match {
      case Success(a) => a
      case Error(e) => sys.error(e.toString)
    }
}