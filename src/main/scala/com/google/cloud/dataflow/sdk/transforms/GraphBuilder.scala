package com.google.cloud.dataflow.sdk.transforms

import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.values.{PCollection, PInput, POutput}

class GraphBuilder(var pipeline: Pipeline) {
  def build(dlist: DList[_]) = {

  }

  def textIO(d: TextDList): PCollection[String] = {
    val trans = TextIO.Read.from(d.pattern)
    val source = d.name match {
      case Some(n) => trans.named(n)
      case None => trans
    }
    pipeline.apply(source)
  }

  def mappedDList[I, O](d: MappedDList[I, O], p: PCollection[I])
  : PCollection[O] = {
    val trans = ParDo.of(new SDoFn[I, O]() {
      override def processElement(c: ProcessContext): Unit = {
        c.output(d.f(c.element()))
      }
    }).named("ScalaMapTransformed")
    p.apply(trans)
  }

  def flatMappedDList[I, O](d: FlatMappedDList[I, O], p: PCollection[I])
  : PCollection[O] = {
    val trans = ParDo.of(new SDoFn[I, O]() {
      override def processElement(c: ProcessContext): Unit = {
        val outputs = d.f(c.element())
        for (o <- outputs) {
          c.output(o)
        }
      }
    }).named("ScalaFlatMapTransformed")
    p.apply(trans)
  }
}