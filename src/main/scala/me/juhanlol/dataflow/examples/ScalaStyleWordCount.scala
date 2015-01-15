package me.juhanlol.dataflow.examples

import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory
import com.google.cloud.dataflow.sdk.transforms._
import com.google.cloud.dataflow.sdk.values.KV
import me.juhanlol.dataflow.DataflowJob


object ScalaStyleWordCount extends App {
  implicit def kvToTuple2[I, O](kv: KV[I, O]): (I, O) = {
    (kv.getKey, kv.getValue)
  }

  // pipeline definition
  val options = PipelineOptionsFactory
    .fromArgs(args)
    .withValidation()
    .as(classOf[WordCountOptions])
  val job = new DataflowJob(options)

  // input
  val input = job.text(options.getInput())

  // transformations
  val words = input.flatMap(line => line.split("[^a-zA-Z']+"))
  val wordCounts = words.applyTransform(Count.perElement())
  val results = wordCounts.map(count => count._1 + "\t" + count._2.toString)

  // output
  results.persist(options.getOutput(), Some("writeCounts"))

  job.run
}