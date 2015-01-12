package me.juhanlol.dataflow

import org.scalatest.FunSuite
import scala.reflect.runtime.universe._

class SKV[K, V]

class TypeResolverTest extends FunSuite {
  test("SimpleType") {
    val resolved = TypeResolver.resolve[Int]()
    assert(resolved == (typeOf[Int], Nil))
  }

  test("ParameterizedType") {
    val resolved = TypeResolver.resolve[SKV[Int, String]]()
    assert(resolved._1 =:= typeOf[SKV[_, _]])
    assert(resolved._2 == List(typeOf[Int], typeOf[String]))
  }
}
