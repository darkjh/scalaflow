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
  implicit def mapFuncToPTransform[I: ClassTag, O: ClassTag](func: I => O)
  : PTransform[PCollection[_ <: I], PCollection[O]] = {
    ParDo.of(new SDoFn[I, O]() {
      override def processElement(c: ProcessContext): Unit = {
        c.output(func(c.element()))
      }
    })
  }

  implicit def flatMapFuncToPTransform[I: ClassTag, O: ClassTag](func: I => Iterable[O])
//  : PTransform[PCollection[_ <: I], PCollection[O]] = {
  : ParDo.Bound[I, O] = {
    ParDo.of(new SDoFn[I, O]() {
      override def processElement(c: ProcessContext): Unit = {
        val outputs = func(c.element())
        for (o <- outputs) {
          c.output(o)
        }
      }
    }).named("ScalaTransformed")
  }

  implicit def kvToTuple2[I, O](kv: KV[I, O]): (I, O) = {
    (kv.getKey, kv.getValue)
  }

  def extractWords(line: String): Iterable[String] = {
    // Split the line into words.
    line.split("[^a-zA-Z']+")
  }

  def formatCounts(count: (String, java.lang.Long)): String = {
    count._1 + "\t" + count._2.toString
  }


  val options = PipelineOptionsFactory
    .fromArgs(args)
    .withValidation()
    .as(classOf[WordCountOptions])
  val p = Pipeline.create(options)

  // input
  val input = p.apply(TextIO.Read.named("ReadLines")
    .from(options.getInput()))

  // transformations
  val trans = ParDo.of(new ExtractWordsFn())
//  println(trans.getFn.getInputTypeToken)
//  println(trans.getFn.getOutputTypeToken)
  println("============================")
  val trans2 = flatMapFuncToPTransform(extractWords)
  println(trans2.getFn)
  println(trans2.getFn.getClass)
//  println(trans2.getFn.getInputTypeToken)
//  println(trans2.getFn.getOutputTypeToken)

  val words = input.apply(trans2)
  val wordCounts = words.apply(Count.perElement())
  val results = wordCounts.apply(ParDo.of(new FormatCountsFn()))

  // output
  results.apply(TextIO.Write.named("WriteCounts")
    .to(options.getOutput()))

  p.run
}