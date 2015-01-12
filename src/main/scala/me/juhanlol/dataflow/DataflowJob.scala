package me.juhanlol.dataflow

import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.coders.{VarIntCoder, CoderRegistry => JCoderRegistry, Coder}
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.PipelineOptions
import com.google.cloud.dataflow.sdk.transforms.Create
import com.google.common.reflect.TypeToken

import scala.collection.JavaConversions
import scala.reflect.ClassTag

class DataflowJob(val pipeline: Pipeline) {
  // setup custom coder registry
  val coders = new JCoderRegistry
  coders.registerStandardCoders()
  // TODO compat scala/java types
  // TODO once for all coder, kryo serialization ???
  coders.registerCoder(classOf[Int], classOf[VarIntCoder])
  pipeline.setCoderRegistry(coders)

  def this(options: PipelineOptions) = {
    this(Pipeline.create(options))
  }

  def run = pipeline.run

  def text(path: String): DList[String] = {
    new DList(pipeline.apply(
      TextIO.Read.named("TextFrom %s".format(path)).from(path)))
  }

  def of[T: ClassTag](iter: Iterable[T]): DList[T] = {
    // get the concrete type
    val clazz = implicitly[ClassTag[T]].runtimeClass
    // get the coder
    val coder = this.coders.getDefaultCoder(
      TypeToken.of(clazz)).asInstanceOf[Coder[T]]
    val pcollection = pipeline.apply(
      Create.of(JavaConversions.asJavaIterable(iter))).setCoder(coder)
    new DList(pcollection)
  }
}