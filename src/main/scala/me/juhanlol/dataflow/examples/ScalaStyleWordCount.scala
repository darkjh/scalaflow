package me.juhanlol.dataflow.examples

import com.google.cloud.dataflow.sdk.Pipeline

import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory
import com.google.cloud.dataflow.sdk.transforms._
import com.google.cloud.dataflow.sdk.values.KV
import me.juhanlol.dataflow.DList


object ScalaStyleWordCount extends App {
  implicit def kvToTuple2[I, O](kv: KV[I, O]): (I, O) = {
    (kv.getKey, kv.getValue)
  }

  // pipeline definition
  val options = PipelineOptionsFactory
    .fromArgs(args)
    .withValidation()
    .as(classOf[WordCountOptions])
  val p = Pipeline.create(options)

  // input
  val input = DList.text(options.getInput(), Some(p))

  // transformations
  val words = input.flatMap(line => line.split("[^a-zA-Z']+"))
  val wordCounts = words.apply(Count.perElement())
  val results = wordCounts.map(count => count._1 + "\t" + count._2.toString)

  // output
  results.persist(options.getOutput(), Some("writeCounts"))

  p.run
}