package me.juhanlol.dataflow

import scala.reflect.runtime.universe._


case class ResolvedTypeInfo(raw: Type, numArgs: Int, actualTypeArgs: List[Type])

// TODO refactor this util
object TypeResolver {
  /**
   * Resolve a type with its type parameters (if any)
   */
  def resolve[T: TypeTag](): ResolvedTypeInfo = {
    val tpe = typeOf[T]
    val numArgs = tpe match {
      case t: ExistentialType => t.quantified.length
      case TypeRef(_, _, args) => args.length
      case _ => 0
    }
    val raw = tpe.erasure
    val typeArgs = this.typeArgs(tpe)
    ResolvedTypeInfo(raw, numArgs, typeArgs)
  }

  def typeArgs(tp: Type): List[Type] = tp match {
    case TypeRef(_, _, args) => args
    case _ => Nil
  }
}