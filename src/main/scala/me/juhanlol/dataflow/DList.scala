package me.juhanlol.dataflow

import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.transforms._
import com.google.cloud.dataflow.sdk.values.{KV, PCollection}
import com.twitter.chill.ClosureCleaner

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

  def by[K: TypeTag](f: T => K): PairDList[K, T] = {
    val func = DList.clean(f)
    val coder = coderRegistry.getDefaultCoder[KV[K, T]]
    val keyed = this.map(elem => KV.of(func(elem), elem))
    val next = keyed.native.setCoder(coder)
    new PairDList[K, T](next, coderRegistry)
  }

  def groupBy[K: TypeTag](f: T => K): PairDList[K, java.lang.Iterable[T]] = {
    val keyed = this.map(elem => KV.of(f(elem), elem))
    val next = keyed.native.apply(GroupByKey.create())
    new PairDList(next, coderRegistry)
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


class PairDList[K: TypeTag, V: TypeTag]
(override val native: PCollection[KV[K, V]],
 override val coderRegistry: CoderRegistry)
  extends DList[KV[K, V]](native, coderRegistry) {

  def countByKey(): PairDList[K, Long] = {
    this.map(kv => kv.getKey).applyTransform(Count.perElement()).asInstanceOf[PairDList[K, Long]]
  }
}


object DList {
  implicit def dlistToPCollection[T: TypeTag](d: DList[T]): PCollection[T] = d.native

  def clean[F <: AnyRef](f: F): F = {
    ClosureCleaner(f)
    f
  }
}