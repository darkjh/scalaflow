package me.juhanlol.dataflow.examples

import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options._
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath
import com.google.cloud.dataflow.sdk.transforms._
import com.google.cloud.dataflow.sdk.values.{PCollection, KV}


/**
 * Returns gs://${STAGING_LOCATION}/"counts.txt" as the default destination.
 */
class OutputFactory extends DefaultValueFactory[String] {
  override def create(options: PipelineOptions): String = {
    val dataflowOptions = options.as(classOf[DataflowPipelineOptions])
    if (dataflowOptions.getStagingLocation != null) {
      GcsPath.fromUri(dataflowOptions.getStagingLocation)
        .resolve("counts.txt").toString
    } else {
      throw new IllegalArgumentException("Must specify --output or --stagingLocation")
    }
  }
}


abstract class SDoFn[I, O] extends DoFn[I, O] {
  type Context = DoFn[I, O]#Context
  type ProcessContext = DoFn[I, O]#ProcessContext
}


/** A DoFn that tokenizes lines of text into individual words. */
class ExtractWordsFn extends SDoFn[String, String] {
  var emptyLines: Aggregator[java.lang.Long] = _

  override def startBundle(c: Context) = {
    emptyLines = c.createAggregator("emptyLines", new Sum.SumLongFn())
  }

  override def processElement(c: DoFn[String, String]#ProcessContext) = {
    // Keep track of the number of empty lines.
    // (When using the [Blocking]DataflowPipelineRunner,
    // Aggregators are shown in the monitoring UI.)
    val elem = c.element()
    if (elem.trim.isEmpty) {
      emptyLines.addValue(1L)
    }

    // Split the line into words.
    val words = elem.split("[^a-zA-Z']+")

    // Output each word encountered into the output PCollection.
    for (word <- words) {
      if (!word.isEmpty) {
        c.output(word)
      }
    }
  }
}

/** A DoFn that converts a Word and Count into a printable string. */
class FormatCountsFn extends SDoFn[KV[String, java.lang.Long], String] {
  override def processElement(c: ProcessContext) {
    c.output(c.element().getKey + "\t" + c.element().getValue)
  }
}


/**
 * A PTransform that converts a PCollection containing lines of text into a PCollection of
 * formatted word counts.
 * <p>
 * Although this pipeline fragment could be inlined, bundling it as a PTransform allows for easy
 * reuse, modular testing, and an improved monitoring experience.
 */
class CountWords extends PTransform[PCollection[String], PCollection[String]] {
  override def apply(lines: PCollection[String]) = {
    // Convert lines of text into individual words.
    val words = lines.apply(ParDo.of(new ExtractWordsFn()))

    // Count the number of times each word occurs.
    val wordCounts = words.apply(Count.perElement())
    val results = wordCounts.apply(ParDo.of(new FormatCountsFn()))

    results
  }
}


trait WordCountOptions extends PipelineOptions {
  @Description("Path of the file to read from")
  @Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
  def getInput(): String
  def setInput(value: String)

  @Description("Path of the file to write to")
  @Default.InstanceFactory(classOf[OutputFactory])
  def getOutput(): String
  def setOutput(value: String)

  /**
   * By default (numShards == 0), the system will choose the shard count.
   * Most programs will not need this option.
   */
  @Description("Number of output shards (0 if the system should choose automatically)")
  def getNumShards(): Int
  def setNumShards(value: Int)
}


/**
 * Created by darkjh on 12/19/14.
 */
object WordCount extends App {
  val options = PipelineOptionsFactory.fromArgs(args).withValidation().as(classOf[WordCountOptions])
  val p = Pipeline.create(options)

  p.apply(TextIO.Read.named("ReadLines").from(options.getInput()))
    .apply(new CountWords())
    .apply(TextIO.Write.named("WriteCounts")
    .to(options.getOutput())
    .withNumShards(options.getNumShards()))

  p.run
}