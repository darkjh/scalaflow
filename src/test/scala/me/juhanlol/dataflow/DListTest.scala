package me.juhanlol.dataflow

import com.google.cloud.dataflow.sdk.testing.{DataflowAssert, TestPipeline}
import com.google.common.reflect.TypeToken
import org.scalatest.FunSuite


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

    DataflowAssert.that(result).containsInAnyOrder(1, 2, 3, 4)
    job.run
  }
}