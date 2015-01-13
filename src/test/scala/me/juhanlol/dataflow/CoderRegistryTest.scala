package me.juhanlol.dataflow

import com.google.cloud.dataflow.sdk.coders.{KvCoder, StringUtf8Coder, VarIntCoder}
import com.google.cloud.dataflow.sdk.values.KV
import org.scalatest.FunSuite


class CoderRegistryTest extends FunSuite {
  test("ScalaSimpleType") {
    val coders = new CoderRegistry

    assert(coders.getDefaultCoder[Int] == VarIntCoder.of())
    assert(coders.getDefaultCoder[String] == StringUtf8Coder.of())
  }

  test("JavaSimpleType") {
    val coders = new CoderRegistry

    assert(coders.getDefaultCoder[Integer] == VarIntCoder.of())
    assert(coders.getDefaultCoder[java.lang.String] == StringUtf8Coder.of())
  }

  test("SimpleParametrizedType") {
    val coders = new CoderRegistry

    assert(coders.getDefaultCoder[KV[Int, String]] ==
      KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()))
  }
}