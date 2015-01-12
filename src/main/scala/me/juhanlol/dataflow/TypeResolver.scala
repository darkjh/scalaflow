package me.juhanlol.dataflow

import scala.reflect.runtime.universe._

object TypeResolver {
  /**
   * Resolve a type with its type parameters (if any)
   */
  def resolve[T: TypeTag](): (Type, List[Type]) = {
    val tpe = typeOf[T]
    val raw = tpe.erasure
    val typeArgs = this.typeArgs(tpe)
    (raw, typeArgs)
  }

  def typeArgs(tp: Type): List[Type] = tp match {
    case TypeRef(_, _, args) => args
    case _ => Nil
  }
}