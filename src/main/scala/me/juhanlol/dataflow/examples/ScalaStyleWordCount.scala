package me.juhanlol.dataflow.examples

import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory
import com.google.cloud.dataflow.sdk.transforms.{PTransform, Count, ParDo}


/**
 * Created by darkjh on 12/22/14.
 */
object ScalaStyleWordCount extends App {
  val options = PipelineOptionsFactory
    .fromArgs(args)
    .withValidation()
    .as(classOf[WordCountOptions])
  val p = Pipeline.create(options)

  // input
  val input = p.apply(TextIO.Read.named("ReadLines")
    .from(options.getInput()))

  // transformations
  val words = input.apply(ParDo.of(new ExtractWordsFn()))
  val wordCounts = words.apply(Count.perElement())
  val results = wordCounts.apply(ParDo.of(new FormatCountsFn()))

  // output
  results.apply(TextIO.Write.named("WriteCounts")
    .to(options.getOutput())
    .withNumShards(options.getNumShards()))

  p.run
}