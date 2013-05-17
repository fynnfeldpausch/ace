/**
 * Ace API
 *
 * Use the Cypher object's apply method to start a Neo4j cypher query.
 *
 * {{{
 * import ace._
 *
 * Cypher("START root=node(0) RETURN root")
 * }}}
 */
package org.neo4j

import scala.language.implicitConversions

package object ace {

  implicit def cypherToSimple(query: CypherQuery): SimpleCypher[Row] = query.asSimple

  implicit def dbValueToOption[T](dbValue: DbValue[T]): Option[T] = dbValue.toOption
  implicit def optionToDbValue[T](option: Option[T]): DbValue[T] = option match {
    case Some(x) => Assigned(x)
    case None => Unassigned
  }
}