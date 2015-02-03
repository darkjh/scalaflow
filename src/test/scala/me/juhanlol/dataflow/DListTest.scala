package me.juhanlol.dataflow

import com.google.cloud.dataflow.sdk.testing.DataflowAssert
import com.google.cloud.dataflow.sdk.values.KV
import org.scalatest.FunSuite

import scala.collection.JavaConversions._


class DListTest extends FunSuite {
  test("CreationFromList") {
    val job = TestJob()
    val data = job.of(List(1, 2, 3, 4))

    DataflowAssert.that(data).containsInAnyOrder(1, 2, 3, 4)
    job.run
  }

  test("Map") {
    val job = TestJob()
    val data = job.of(List("1", "2", "3"))
    val result = data.map(e => new Integer(e.toInt))

    DataflowAssert.that(result).containsInAnyOrder(1, 2, 3)
    job.run
  }

  test("Filter") {
    val job = TestJob()
    val data = job.of(List(1, 2, 3, 4, 5))
    val result = data.filter(_ > 3)

    DataflowAssert.that(result).containsInAnyOrder(4, 5)
    job.run
  }

  test("FlatMap") {
    val job = TestJob()
    val data = job.of(List(2, 3, 4))
    val result = data.flatMap(e => 1 to e)

    DataflowAssert.that(result).containsInAnyOrder(1, 2, 1, 2, 3, 1, 2, 3, 4)
    job.run
  }

  test("By") {
    val job = TestJob()
    val data = job.of(List(1, 3, 9))
    val kv = data.by(_ + 1)

    DataflowAssert.that(kv).containsInAnyOrder(
      KV.of(2, 1), KV.of(4, 3), KV.of(10, 9)
    )
    job.run
  }

  test("GroupBy") {
    val job = TestJob()
    val data = job.of(List("hello", "hi", "apple", "banana"))
    val result = data.groupBy(_.charAt(0).toString)

    DataflowAssert.that(result).containsInAnyOrder(
      KV.of("h", List("hello", "hi")),
      KV.of("a", List("apple")),
      KV.of("b", List("banana"))
    )
  }

  test("CountByKey") {
    val job = TestJob()
    val data = job.of(List("a", "c", "b", "c", "c"))
    val kv = data.map(x => KV.of(x, 1))
    val result = kv.countByKey()

    DataflowAssert.that(result).containsInAnyOrder(
      KV.of("a", 1l),
      KV.of("b", 1l),
      KV.of("c", 3l)
    )
  }
}