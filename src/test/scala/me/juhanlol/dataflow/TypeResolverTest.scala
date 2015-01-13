package me.juhanlol.dataflow

import org.scalatest.FunSuite
import scala.reflect.runtime.universe._

class SKV[K, V]

class TypeResolverTest extends FunSuite {
  test("SimpleType") {
    val resolved = TypeResolver.resolve[Int]()
    assert(resolved.raw =:= typeOf[Int])
    assert(resolved.numArgs == 0)
    assert(resolved.actualTypeArgs == Nil)
  }

  test("ParameterizedType") {
    val resolved = TypeResolver.resolve[SKV[Int, String]]()
    assert(resolved.raw =:= typeOf[SKV[_, _]])
    assert(resolved.numArgs == 2)
    assert(resolved.actualTypeArgs == List(typeOf[Int], typeOf[String]))
  }

  test("RawParameterizedType") {
    val resolved = TypeResolver.resolve[SKV[_, _]]()
    assert(resolved.raw =:= typeOf[SKV[_, _]])
    assert(resolved.numArgs == 2)
    assert(resolved.actualTypeArgs == Nil)
  }
}
