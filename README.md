# Anorm for Cypher Embedded!

The [play!](http://www.playframework.org/) framework includes a simple data access layer called [Anorm](http://www.playframework.org/documentation/2.0.4/ScalaAnorm). It uses plain SQL to interact with the database and provides an API to parse and transform the resulting datasets.
Some libraries exist to transfer this concept to the world of [Neo4j](http://www.neo4j.org/). The [Neo4j Scala wrapper](https://github.com/FaKod/neo4j-scala/) has introduced basic Cypher query support and [AnormCypher](http://anormcypher.org/) provides a Anorm-like API purely for REST.

However, if you want to use Neo4j embedded in your applications you will be in need of another library. **Ace** (i.e. **A**norm for **C**ypher **E**mbedded) closes this gap and provides a data access layer that uses plain Cypher to interact with the database. Ace (much like AnormCypher) is modeled after Anorm for play! and it's API will look extremly similar.
If you have worked with Anorm before, you will find Ace very easy to understand and work with.

## Executing Cypher queries

To start you need to learn how to execute simple Cypher queries.

First, import `org.ace._`, and then simply use the `Cypher` object to create queries. You need to provide an implicit `GraphDatabaseService` to run a query:

```scala
import org.ace._

Neo4j.withTx { implicit service =>
  val result: Boolean = Cypher("START n=node(0) RETURN n").execute()    
} 
```

The `execute()` method returns a boolean value indicating whether the execution was successful.

To execute an update, you can use `executeUpdate()`, which returns a 5-tuple containing:
 1. The number of nodes created.
 2. The number of relationships created.
 3. The number of properties set.
 4. The number of deleted nodes.
 5. The number of deleted relationships.

```scala
val (nodesCreated, relsCreated, propsSet, nodesDeleted, relsDeleted) =
  Cypher("START n=node(0) MATCH n-[r]-() DELETE n, r").executeUpdate()
```

Since Scala supports multi-line strings, feel free to use them for complex Cypher statements:

```scala
val cypherQuery = Cypher(
  """
    START n=node(0)
    MATCH (n)--(x)
    RETURN x
  """
)
```

Cypher supports querying with parameters. If your Cypher query needs dynamic parameters, you can assign them as follows:

```scala
Cypher(
  """
    START n=node(0,1,2)
    RETURN n
    SKIP {s}
    LIMIT {l}
  """
).on("s" -> 1, "l" -> 1)
```

## Retrieving data using the Stream API

The first way to access the results of a select query is to use the Stream API.

When you call `apply()` on any Cypher statement, you will receive a `Stream` of `Row` instances, where each row can be seen as a dictionary:

```scala
// Create a Cypher query
val selectQuery = Cypher("START n=node(0) RETURN n.name, n.value")
 
// Transform the resulting Stream[Row] to a List[(String,String)]
val selection = selectQuery().map(row => 
  row[String]("n.name") -> row[String]("n.value")
).toList
```

In the following example we will count the number of connected nodes from the start node, so the result set will be a single row with a single column:

```scala
// First retrieve the first row
val firstRow = Cypher("START n=node(2) MATCH (n)-->(x) RETURN count(x) AS c").apply().head
 
// Next get the content of the 'c' column as Long
val nodeCount = firstRow[Long]("c")
```

## Using Pattern Matching

You can also use Pattern Matching to match and extract the `Row` content. In this case the column name doesn’t matter. Only the order and the type of the parameters is used to match.

The following example transforms each row to the correct Scala type:

```scala
case class SmallCountry(name:String) 
case class BigCountry(name:String) 
case class France
 
val countries = Cypher("START n=node(*) WHERE n.type! = 'Country' RETURN n.name as name, n.population as pop")().collect {
  case Row("France", _) => France()
  case Row(name:String, pop:Int) if(pop > 1000000) => BigCountry(name)
  case Row(name:String, _) => SmallCountry(name)      
}
```

Note that since `collect(…)` ignores the cases where the partial function isn’t defined, it allows your code to safely ignore rows that you don’t expect.

## Dealing with Nullable columns

If a column can contain `Null` values in the database schema, you need to manipulate it as an `Option` type.

For example, the `indepYear` of the `Country` table is nullable, so you need to match it as `Option[Int]`:

```scala
Cypher("START n=node(*) WHERE n.type! = 'Country' RETURN n.name AS name, n.indepYear? AS year")().collect {
  case Row(name:String, Some(year:Int)) => name -> year
}
```

If you try to match this column as `Int` it won’t be able to parse `Null` values. Suppose you try to retrieve the column content as `Int` directly from the dictionary:

```scala
Cypher("START n=node(*) WHERE n.type! = 'Country' RETURN n.name AS name, n.indepYear? AS year")().map { row =>
  row[String]("name") -> row[Int]("year")
}
```

This will produce an `UnexpectedNullableFound(year)` exception if it encounters a null value, so you need to map it properly to an `Option[Int]`, as:

```scala
Cypher("START n=node(*) WHERE n.type! = 'Country' RETURN n.name AS name, n.indepYear? AS year")().map { row =>
  row[String]("name") -> row[Option[Int]]("year")
}
```

This is also true for the parser API, as we will see next.

## Using the Parser API

You can use the parser API to create generic and reusable parsers that can parse the result of any select query.

> **Note:** This is really useful, since most queries in a web application will return similar data sets. For example, if you have defined a parser able to parse a `Country` from a result set, and another `Language` parser, you can then easily compose them to parse both Country and Language from a join query.
>
> First you need to `import org.ace.CypherParser._`

### Getting a single result

First you need a `RowParser`, i.e. a parser able to parse one row to a Scala value. For example we can define a parser to transform a single column result set row, to a Scala `Long`:

```scala
val rowParser = scalar[Long]
```

Then we have to transform it into a `ResultSetParser`. Here we will create a parser that parse a single row:

```scala
val rsParser = scalar[Long].single
```

So this parser will parse a result set to return a `Long`. It is useful to parse to result produced by a simple Cypher `COUNT` query:

```scala
val count: Long = Cypher("START n=node(*) return COUNT(n)").as(scalar[Long].single)
```

### Getting a single optional result

Let's say you want to retrieve a value from a query that might return null. We'll use the singleOpt parser:

```scala
val countryId: Option[Long] = Cypher("START n=node(*) WHERE n.type! = 'Country' AND n.country = 'France' RETURN ID(n)").as(scalar[Long].singleOpt)
```

## Handling Neo4j types

The following parsers will retrieve Neo4j nodes and relationships from a query:

```scala
val node: org.neo4j.graphdb.Node = Cypher("START n=node(0) RETURN n").as(node("n").single)
val rels: org.neo4j.graphdb.Relationship = Cypher("START n=node(0) MATCH n-[r]->() RETURN r").as(relationship("r") *)
```

If you need to retrieve Cypher paths, you can use the `path` parser. It will result in a `Seq[org.neo4j.graphdb.PropertyContainer]` containing all nodes and relationships along the given path. These will usually emerge in alternate order.

```scala
val path: Seq[org.neo4j.graphdb.PropertyContainer] = Cypher("START n=node(0) MATCH p=n-[r]-m RETURN p").as(path("p") *)
```

# References

 * [Anorm](http://www.playframework.org/documentation/2.0.4/ScalaAnorm) - simple SQL data access
 * [AnormCypher](http://anormcypher.org/) - a Neo4j library purely for REST
 * [Play!](http://www.playframework.org/) - a framework for web applications with Java & Scala
 * [Neo4j](http://www.neo4j.org/) - an open-source, high-performance, enterprise-grade NOSQL graph database
