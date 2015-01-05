package me.juhanlol.dataflow

import com.google.cloud.dataflow.sdk.testing.{DataflowAssert, TestPipeline}
import org.scalatest.FunSuite


class DListTest extends FunSuite {
  test("CreationFromList") {
    val p = TestPipeline.create()
    val data = DList.of(List(1, 2, 3, 4), Some(p))

    DataflowAssert.that(data).containsInAnyOrder(1, 2, 3, 4)
    p.run
  }
}