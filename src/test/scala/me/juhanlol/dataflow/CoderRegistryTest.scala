package me.juhanlol.dataflow

import com.google.cloud.dataflow.sdk.coders.{KvCoder, StringUtf8Coder, VarIntCoder}
import com.google.cloud.dataflow.sdk.values.KV
import org.scalatest.{BeforeAndAfter, FunSuite}


class CoderRegistryTest extends FunSuite with BeforeAndAfter {
  var coders: CoderRegistry = _

  before {
    coders = new CoderRegistry
  }

  test("ScalaSimpleType") {
    assert(coders.getDefaultCoder[Int] == VarIntCoder.of())
    assert(coders.getDefaultCoder[String] == StringUtf8Coder.of())
  }

  test("JavaSimpleType") {
    assert(coders.getDefaultCoder[Integer] == VarIntCoder.of())
    assert(coders.getDefaultCoder[java.lang.String] == StringUtf8Coder.of())
  }

  test("SimpleParametrizedType") {
    assert(coders.getDefaultCoder[KV[Int, String]] ==
      KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()))
  }

  test("NestedParametrizedType") {
    assert(coders.getDefaultCoder[KV[String, KV[Int, Int]]] ==
      KvCoder.of(StringUtf8Coder.of(),
        KvCoder.of(VarIntCoder.of(), VarIntCoder.of())))
  }
}