package me.juhanlol.dataflow.examples

import com.google.cloud.dataflow.sdk.options.{Default, Description, PipelineOptions}


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
