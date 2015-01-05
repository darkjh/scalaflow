package me.juhanlol.dataflow

import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.coders.{VarIntCoder, CoderRegistry, Coder}
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory
import com.google.cloud.dataflow.sdk.transforms.{Create, GroupByKey, PTransform, ParDo}
import com.google.cloud.dataflow.sdk.values.{KV, PCollection}
import com.google.common.reflect.TypeToken
import com.twitter.chill.ClosureCleaner

import scala.collection.JavaConversions
import scala.reflect.ClassTag


class DList[T: ClassTag](val native: PCollection[T]) {
  def apply[U: ClassTag](trans: PTransform[PCollection[T], PCollection[U]])
  : DList[U] = {
    new DList[U](native.apply(trans))
  }

  def map[U: ClassTag](f: T => U): DList[U] = {
    val func = DList.clean(f)
    val trans = ParDo.of(new SDoFn[T, U]() {
      override def processElement(c: ProcessContext): Unit = {
        c.output(func(c.element()))
      }
    }).named("ScalaMapTransformed")

    new DList[U](native.apply(trans))
  }

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): DList[U] = {
    val func = DList.clean(f)
    val trans = ParDo.of(new SDoFn[T, U]() {
      override def processElement(c: ProcessContext): Unit = {
        val outputs = func(c.element())
        for (o <- outputs) {
          c.output(o)
        }
      }
    }).named("ScalaFlatMapTransformed")
    new DList[U](native.apply(trans))
  }

  def filter(f: T => Boolean): DList[T] = {
    val func = DList.clean(f)
    val trans = ParDo.of(new SDoFn[T, T]() {
      override def processElement(c: ProcessContext): Unit = {
        if (func(c.element())) {
          c.output(c.element())
        }
      }
    }).named("ScalaFilterTransformed")
    new DList[T](native.apply(trans))
  }

  def by[K](f: T => K): PairDList[K, T] = {
    val func = DList.clean(f)
    val keyed = this.map(elem => KV.of(func(elem), elem))
    new PairDList[K, T](keyed.native)
  }

  def groupBy[K](f: T => K): PairDList[K, java.lang.Iterable[T]] = {
    val keyed = this.map(elem => KV.of(f(elem), elem))
    new PairDList(keyed.native.apply(GroupByKey.create[K, T]()))
  }

  def persist(path: String, name: Option[String] = None): Unit = {
    val trans = TextIO.Write.named(name.getOrElse("Persist")).to(path)
    // TODO how to remove this cast ???
    native.asInstanceOf[PCollection[String]].apply(trans)
  }
}


class PairDList[K, V](override val native: PCollection[KV[K, V]])
  extends DList[KV[K, V]](native) {
}


object DList {
  implicit def pcollectionToDList[T: ClassTag](p: PCollection[T]): DList[T] = new DList[T](p)
  implicit def dlistToPCollection[T: ClassTag](d: DList[T]): PCollection[T] = d.native

  val coders = new CoderRegistry
  coders.registerStandardCoders()
  // TODO compat scala/java types
  coders.registerCoder(classOf[Int], classOf[VarIntCoder])

  def clean[F <: AnyRef](f: F): F = {
    ClosureCleaner(f)
    f
  }

  def of[T: ClassTag](iter: Iterable[T], pipeline: Option[Pipeline] = None): DList[T] = {
    // get the pipeline or create a default one
    val p = pipeline.getOrElse(Pipeline.create(PipelineOptionsFactory.create()))
    // get the concrete type
    val clazz = implicitly[ClassTag[T]].runtimeClass
    // get the coder
    val coder = DList.coders.getDefaultCoder(TypeToken.of(clazz)).asInstanceOf[Coder[T]]
    val pcollection = p.apply(Create.of(JavaConversions.asJavaIterable(iter))).setCoder(coder)
    new DList(pcollection)
  }

  def text(path: String, pipeline: Option[Pipeline] = None): DList[String] = {
    // get the pipeline or create a default one
    val p = pipeline.getOrElse(Pipeline.create(PipelineOptionsFactory.create()))
    new DList(p.apply(TextIO.Read.named("TextFrom %s".format(path)).from(path)))
  }

  // TODO avro
}