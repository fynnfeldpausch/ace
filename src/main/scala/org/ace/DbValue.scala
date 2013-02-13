package org.ace

abstract class DbValue[+A] {

  def toOption: Option[A] = this match {
    case Assigned(x) => Some(x)
    case Unassigned => None
  }

  def isDefined: Boolean = toOption.isDefined
  def get: A = toOption.get
  def getOrElse[V >: A](id: V): V = toOption.getOrElse(id)
  def map[B](f: A => B) = toOption.map(f)
  def flatMap[B](f: A => Option[B]) = toOption.flatMap(f)
  def foreach(f: A => Unit) = toOption.foreach(f)
}

case class Assigned[A](value: A) extends DbValue[A] {
  override def toString() = value.toString
}

case object Unassigned extends DbValue[Nothing] {
  override def toString() = "Unassigned"
}