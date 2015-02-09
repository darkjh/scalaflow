package me.juhanlol.dataflow

import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.transforms._
import com.google.cloud.dataflow.sdk.transforms.join.{CoGbkResult, CoGroupByKey, KeyedPCollectionTuple}
import com.google.cloud.dataflow.sdk.values.{TupleTag, KV, PCollection}
import com.twitter.chill.ClosureCleaner

import scala.collection.JavaConverters._

import scala.reflect.runtime.universe._

/**
 * DList (Distributed List) is the main abstraction of Scalaflow,
 * it's similar to Spark's RDD. Operations on DList is transformed into
 * Dataflow's transformation and could be executed locally or in
 * Google's managed cloud service
 *
 * @tparam T Type contained in DList, should have a corresponding coder
 *           registered
 */
class DList[T: TypeTag](val native: PCollection[T],
                        val coderRegistry: CoderRegistry) {
  /**
   * Apply an existing PTransform to the DList (eg. Count.perElement())
   */
  def applyTransform[U: TypeTag](trans: PTransform[PCollection[T], PCollection[U]])
  : DList[U] = {
    new DList[U](native.apply(trans), this.coderRegistry)
  }

  /**
   * Map the specified function on the DList
   */
  def map[U: TypeTag](f: T => U): DList[U] = {
    val func = DList.clean(f)
    val coder = coderRegistry.getDefaultCoder[U]
    val trans = ParDo.of(new SDoFn[T, U]() {
      override def processElement(c: ProcessContext): Unit = {
        c.output(func(c.element()))
      }
    }).named("ScalaMapTransformed")
    val next = native.apply(trans).setCoder(coder)

    new DList[U](next, this.coderRegistry)
  }

  /**
   * FlatMap (map then flatten) the specified function on the DList
   */
  def flatMap[U: TypeTag](f: T => TraversableOnce[U]): DList[U] = {
    val func = DList.clean(f)
    val coder = coderRegistry.getDefaultCoder[U]
    val trans = ParDo.of(new SDoFn[T, U]() {
      override def processElement(c: ProcessContext): Unit = {
        val outputs = func(c.element())
        for (o <- outputs) {
          c.output(o)
        }
      }
    }).named("ScalaFlatMapTransformed")
    val next = native.apply(trans).setCoder(coder)
    new DList[U](next, this.coderRegistry)
  }

  /**
   * Return a new DList with elements that satisfy the predicate
   */
  def filter(f: T => Boolean): DList[T] = {
    val func = DList.clean(f)
    val trans = ParDo.of(new SDoFn[T, T]() {
      override def processElement(c: ProcessContext): Unit = {
        if (func(c.element())) {
          c.output(c.element())
        }
      }
    }).named("ScalaFilterTransformed")
    new DList[T](native.apply(trans), this.coderRegistry)
  }

  def by[K: TypeTag](f: T => K): DList[KV[K, T]] = {
    val func = DList.clean(f)
    val coder = coderRegistry.getDefaultCoder[KV[K, T]]
    val keyed = this.map(elem => KV.of(func(elem), elem))
    val next = keyed.native.setCoder(coder)
    new DList(next, coderRegistry)
  }

  def groupBy[K: TypeTag](f: T => K): DList[KV[K, java.lang.Iterable[T]]] = {
    val keyed = this.map(elem => KV.of(f(elem), elem))
    val next = keyed.native.apply(GroupByKey.create())
    new DList(next, coderRegistry)
  }

  /**
   * Persist the DList on storage
   * Only text format is supported for the moment
   */
  def persist(path: String, name: Option[String] = None): Unit = {
    val trans = TextIO.Write.named(name.getOrElse("Persist")).to(path)
    // TODO how to remove this cast ???
    native.asInstanceOf[PCollection[String]].apply(trans)
  }
}


class PairDListFunctions[K: TypeTag, V: TypeTag](self: DList[KV[K, V]]) {
  def countByKey(): DList[KV[K, java.lang.Long]] = {
    self.map(kv => kv.getKey).applyTransform(Count.perElement())
  }

  def join[W: TypeTag](that: DList[KV[K, W]]): DList[KV[K, KV[V, W]]] = {
    val tv = new TupleTag[V]()
    val tw = new TupleTag[W]()
    val coGroupResult = KeyedPCollectionTuple.of(tv, self.native)
      .and(tw, that.native)
      .apply(CoGroupByKey.create())

    val result = coGroupResult.apply(ParDo.of(
      new SDoFn[KV[K, CoGbkResult], KV[K, KV[V, W]]] {
        override def processElement(
          c: DoFn[KV[K, CoGbkResult],
            KV[K, KV[V, W]]]#ProcessContext): Unit = {
          val elem = c.element()
          val k = elem.getKey
          val vs = elem.getValue.getAll(tv).asScala
          val ws = elem.getValue.getAll(tw).asScala
          for {
            v <- vs if v != null
            w <- ws if w != null
          } {
            c.output(KV.of(k, KV.of(v, w)))
          }
        }
      }
    ))
    val coder = self.coderRegistry.getDefaultCoder[KV[K, KV[V, W]]]
    result.setCoder(coder)
    new DList(result, self.coderRegistry)
  }
}


object DList {
  implicit def dlistToPCollection[T: TypeTag]
  (d: DList[T]): PCollection[T] = d.native

  implicit def dlistToPairDListFunctions[K: TypeTag, V: TypeTag]
  (dlist: DList[KV[K, V]]): PairDListFunctions[K, V] = {
    new PairDListFunctions(dlist)
  }

  def clean[F <: AnyRef](f: F): F = {
    ClosureCleaner(f)
    f
  }
}