package org.acetest

import org.ace._
import org.ace.CypherParser._
import org.scalatest.{path => stpath, _}
import org.scalatest.matchers.ShouldMatchers
import scala.collection.JavaConverters._

class AceSpec extends FlatSpec
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with ShouldMatchers {

  override def beforeAll(): Unit = {
  	Database.start()
  }

  override def afterAll(): Unit = {
    Database.shutdown()
  }

  override def beforeEach(): Unit = {
    Database.withTx { implicit service =>
      Cypher("""
        CREATE
          (props { tag:"ace", name:"props", lng:42, dbl:3.141, str:"Hello world!", arrLng:[1,2,3], arrDbl:[1.234,2.345,3.456], arrStr:["a","b","c"] }),
          (john { tag:"ace", name:"John", age:21 }),
          (sarah { tag:"ace", name:"Sarah", age:36 }),
          (maria { tag:"ace", name:"Maria", age:43, comment:"I am a node!"}),
          (joe { tag:"ace", name:"Joe", age:65 }),
          (steve { tag:"ace", name:"Steve", age:43 }),
          john -[:friend]-> sarah,
          john -[:friend]-> joe,
          sarah -[:friend { comment:"I am a relationship!" }]-> maria,
          joe -[:friend]-> steve"""
      )()
    }
  }

  override def afterEach(): Unit = {
    Database.withTx { implicit service =>
      Cypher("""
        START n=node(*)
        MATCH n-[r?]-()
        WHERE n.tag! = "ace"
        DELETE n, r"""
      )()
    }
  }

  "Cypher" should "be able to build a Cypher query with apply" in {
    val query = """
      START n=node(*)
      RETURN n"""
    Cypher(query) should equal (CypherQuery(query))
  }

  it should "be able to make a query without parameters" in {
    Database.withTx { implicit service =>
      Cypher("""
        START n=node(*)
        RETURN n"""
      )()
    }
  }

  it should "be able to build a Cypher query and send it with apply" in {
    val data = Database.withTx { implicit service =>
      Cypher("""
        START n=node(*)
        RETURN n"""
      )()
    }
    data.size should equal (7)
  }

  it should "be able to add parameters with .on()" in {
    val query = """
      START n=node(*) 
      WHERE n.name! = {name} OR n.age! = {age}
      RETURN n"""
    val cypher = Cypher(query).on("name" -> "John", "age" -> 43)
    val data = Database.withTx { implicit service =>
      cypher()
    }
    cypher.params should equal (Map("name" -> "John", "age" -> 43))
    data.size should equal (3)
  }

  it should "be able to send a query and map the results to a list" in {
    val data = Database.withTx { implicit service =>
      Cypher("""
        START n=node(*) 
        WHERE n.age! = 43
        RETURN n.name AS name
        ORDER BY name"""
      )().map(row => row[String]("name")).toList
    }
    data should equal (List("Maria", "Steve"))
  }

  it should "be able to submit a few requests in a row" in {
    val (data0, data1, data2, data3) = Database.withTx { implicit service =>
      val query = Cypher("""
        START n=node(*)
        WHERE n.name! = "John"
        RETURN n"""
      )
      (query(), query(), query(), query())
    }
    data1 should equal (data0)
    data2 should equal (data0)
    data3 should equal (data0)
  }

  it should "be able to extract properties of different types" in {
    val data = Database.withTx { implicit service =>
      Cypher("""
        START n=node(*) 
        WHERE n.name! = "props"
        RETURN n.lng, n.dbl, n.str, n.arrLng, n.arrDbl, n.arrStr"""
      )()
    }
    val props = data.map(row => List(
      row[Long]("n.lng"),
      row[Double]("n.dbl"),
      row[String]("n.str"),
      row[Seq[Long]]("n.arrLng"),
      row[Seq[Double]]("n.arrDbl"),
      row[Seq[String]]("n.arrStr")
    )).toList.head
    props should equal (List(
      42, 
      3.141, 
      "Hello world!",
      Seq(1, 2, 3),
      Seq(1.234, 2.345, 3.456),
      Seq("a", "b", "c")
    ))
  }

  it should "be able to .execute() a good query" in {
    val data = Database.withTx { implicit service =>
      Cypher("""
        START n=node(*)
        RETURN n"""
      ).execute()
    }
    data should be (true)
  }

  it should "be able to .execute() a bad query" in {
    val data = Database.withTx { implicit service =>
      Cypher("""
        FIND n=node(*)
        RETURN n"""
      ).execute()
    }
    data should be (false)
  }

  it should "be able to retrieve query statistics with .executeUpdate()" in {
    val data = Database.withTx { implicit service =>
      Cypher("""
        CREATE (peter { tag:"ace", name:"Peter", age:77 })"""
      ).executeUpdate()
    }
    val (ndsAdd, relAdd, prpSet, ndsDel, relDel) = data
    ndsAdd should equal (1)
    relAdd should equal (0)
    prpSet should equal (3)
    ndsDel should equal (0)
    relDel should equal (0)
  }

  it should "be able to parse nullable fields" in {
    val data = Database.withTx { implicit service =>
      Cypher("""
        START n=node(*)
        WHERE n.age! = 43
        RETURN n.comment? as comment
        ORDER BY n.name"""
      )().map(row => row[Option[String]]("comment")).toList
    }
    data should equal (List(Some("I am a node!"), None))
  }

  it should "fail on null fields if they're not Option" in {
    evaluating {
      Database.withTx { implicit service =>
        Cypher("""
          START n=node(*)
          WHERE n.age! = 43
          RETURN n.comment? as comment
          ORDER BY n.name"""
        )().map(row => row[String]("comment")).toList
      }
    } should produce [RuntimeException]
  }

  "CypherParser" should "be able to parse into a single Long" in {
    val data: Long = Database.withTx { implicit service =>
      Cypher("""
        START n=node(*)
        RETURN COUNT(n)"""
      ).as(scalar[Long].single)
    }
    data should equal (7)
  }

  it should "be able to parse a single Node" in {
    val data: org.neo4j.graphdb.Node = Database.withTx { implicit service =>
      Cypher("""
        START n=node(*)
        WHERE n.name! = "Maria"
        RETURN n"""
      ).as(node("n").single)
    }
    data.hasProperty("comment") should be (true)
    data.getProperty("comment") should be ("I am a node!")
  }

  it should "be able to parse a single Relationship" in {
    val data: org.neo4j.graphdb.Relationship = Database.withTx { implicit service =>
      Cypher("""
        START n=node(*)
        MATCH n <-[r:friend]- ()
        WHERE n.name! = "Maria"
        RETURN r"""
      ).as(relationship("r").single)
    }
    data.hasProperty("comment") should be (true)
    data.getProperty("comment") should be ("I am a relationship!")
  }

  it should "be able to parse and flatten into a tuple" in {
    val data = Database.withTx { implicit service =>
      Cypher("""
        START n=node(*)
        WHERE n.age! < 40
        RETURN n.name, n.age
        ORDER BY n.age"""
      ).as(str("n.name") ~ long("n.age") map(flatten) *)
    }
    data should equal (List(("John",21), ("Sarah", 36)))
  }

  it should "be able to parse a collection" in {
    val data = Database.withTx { implicit service =>
      Cypher("""
        START n=node(*)
        WHERE HAS(n.age)
        RETURN collect(n.name?) AS names"""
      ).as(get[Seq[String]]("names").single)
    }
    data should equal (Seq("John", "Sarah", "Maria", "Joe", "Steve"))
  }

  it should "be able to parse a path" in {
    val data = Database.withTx { implicit service =>
      Cypher("""
        START n=node(*)
        MATCH p = n --> m
        WHERE n.name! = "Sarah"
        RETURN p"""
      ).as(path("p").single)
    }
    data.size should equal (3)
    data(0).isInstanceOf[org.neo4j.graphdb.Node] should equal (true)
    data(1).isInstanceOf[org.neo4j.graphdb.Relationship] should equal (true)
    data(2).isInstanceOf[org.neo4j.graphdb.Node] should equal (true)
  }
}