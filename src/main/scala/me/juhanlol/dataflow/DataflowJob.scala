package me.juhanlol.dataflow

import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.PipelineOptions
import com.google.cloud.dataflow.sdk.transforms.Create

import scala.collection.JavaConversions
import scala.reflect.runtime.universe._


/**
 * Context of a dataflow job
 * It handles coder registration and provide methods for creating the initial
 * DList from existing data source
 */
class DataflowJob(val pipeline: Pipeline) {
  val coderRegistry = new CoderRegistry

  def this(options: PipelineOptions) = {
    this(Pipeline.create(options))
  }

  def run = pipeline.run

  /**
   * Create a DList from a data source
   * Only text data is supported for the moment
   */
  def text(path: String): DList[String] = {
    new DList(pipeline.apply(
      TextIO.Read.named("TextFrom %s".format(path)).from(path)),
      coderRegistry)
  }

  /**
   * Create a DList from an in-memory collection, usually for test purpose
   */
  def of[T: TypeTag](iter: Iterable[T]): DList[T] = {
    val coder = coderRegistry.getDefaultCoder[T]
    val pcollection = pipeline.apply(
      Create.of(JavaConversions.asJavaIterable(iter))).setCoder(coder)
    new DList(pcollection, coderRegistry)
  }
}