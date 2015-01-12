package me.juhanlol.dataflow

import org.scalatest.FunSuite

class TestKV[K, V]

class CoderRegistryTest extends FunSuite {
  test("ScalaSimpleType") {
    val coders = new CoderRegistry

    println(coders.getDefaultCoder[Int])
    println(coders.getDefaultCoder[Integer])
    println(coders.getDefaultCoder[String])
  }
}