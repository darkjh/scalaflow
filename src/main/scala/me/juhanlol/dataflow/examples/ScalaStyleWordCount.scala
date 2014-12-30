package me.juhanlol.dataflow.examples

import com.google.cloud.dataflow.sdk.Pipeline

import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory
import com.google.cloud.dataflow.sdk.transforms.{SDoFn, PTransform, Count, ParDo}
import com.google.cloud.dataflow.sdk.values.{KV, PCollection}

import scala.reflect.ClassTag


/**
 * Created by darkjh on 12/22/14.
 */
object ScalaStyleWordCount extends App {
  implicit def mapFuncToPTransform[I: ClassTag, O: ClassTag]
  (func: I => O)
  : PTransform[PCollection[_ <: I], PCollection[O]] = {
    ParDo.of(new SDoFn[I, O]() {
      override def processElement(c: ProcessContext): Unit = {
        c.output(func(c.element()))
      }
    }).named("ScalaMapTransformed")
  }

  implicit def flatMapFuncToPTransform[I: ClassTag, O: ClassTag]
  (func: I => Iterable[O])
  : ParDo.Bound[I, O] = {
    ParDo.of(new SDoFn[I, O]() {
      override def processElement(c: ProcessContext): Unit = {
        val outputs = func(c.element())
        for (o <- outputs) {
          c.output(o)
        }
      }
    }).named("ScalaFlatMapTransformed")
  }

  implicit def kvToTuple2[I, O](kv: KV[I, O]): (I, O) = {
    (kv.getKey, kv.getValue)
  }

  // Transform functions
  /** Split a line into words */
  def extractWords(line: String): Iterable[String] = {
    line.split("[^a-zA-Z']+")
  }

  /** Format result string from counts */
  def formatCounts(count: KV[String, java.lang.Long]): String = {
    count._1 + "\t" + count._2.toString
  }

  // pipeline definition
  val options = PipelineOptionsFactory
    .fromArgs(args)
    .withValidation()
    .as(classOf[WordCountOptions])
  val p = Pipeline.create(options)

  // input
  val input = p.apply(TextIO.Read.named("ReadLines")
    .from(options.getInput()))

  // transformations
  val words = input.apply(flatMapFuncToPTransform(extractWords))
  val wordCounts = words.apply(Count.perElement())
  val results = wordCounts.apply(mapFuncToPTransform(formatCounts))

  // output
  results.apply(TextIO.Write.named("WriteCounts")
    .to(options.getOutput()))

  p.run
}