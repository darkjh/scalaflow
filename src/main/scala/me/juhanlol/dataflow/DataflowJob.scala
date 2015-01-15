package me.juhanlol.dataflow

import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.PipelineOptions
import com.google.cloud.dataflow.sdk.transforms.Create

import scala.collection.JavaConversions
import scala.reflect.runtime.universe._

class DataflowJob(val pipeline: Pipeline) {
  val coderRegistry = new CoderRegistry

  def this(options: PipelineOptions) = {
    this(Pipeline.create(options))
  }

  def run = pipeline.run

  def text(path: String): DList[String] = {
    new DList(pipeline.apply(
      TextIO.Read.named("TextFrom %s".format(path)).from(path)),
      coderRegistry)
  }

  def of[T: TypeTag](iter: Iterable[T]): DList[T] = {
    val coder = coderRegistry.getDefaultCoder[T]
    val pcollection = pipeline.apply(
      Create.of(JavaConversions.asJavaIterable(iter))).setCoder(coder)
    new DList(pcollection, coderRegistry)
  }
}