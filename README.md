Scalaflow
=========

Scalaflow is a Scala DSL for Google's Cloud Dataflow's Java SDK.

Dataflow is a cool service but it's even cooler if we can write data pipelines and distributed data transformations in Scala with a fluent style:

```Scala
object ScalaStyleWordCount extends App {
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
  val results = wordCounts.map(
    count => count.getKey + "\t" + count.getValue.toString)

  // output
  results.persist(options.getOutput(), Some("writeCounts"))

  job.run
}
```

The Java version of this code can be found [here](https://github.com/darkjh/scalaflow/blob/master/src/main/scala/me/juhanlol/dataflow/examples/JavaStyleWordCount.scala). Compare them and make a judge yourself!

### Features ###

  - a `DList` abstraction similar to Spark's RDD
  - a `DataflowJob` abstraction that wraps the `Pipeline` in Java SDK
  - a Scala side `CoderRegistry` for data type serialization and desrialization

### TODOs ###

  - Integrate the KV data operations (groupByKey, Combine, Join etc.)
  - More tests on `CoderRegistry` (port Java SDK's test)
  - Use Kryo for user class ser/deser

Feel free to share your ideas with me on this project: ju.han.felix at gmail.com or @juhanlol on twitter.
