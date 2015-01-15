package me.juhanlol.dataflow

import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.transforms._
import com.google.cloud.dataflow.sdk.values.{KV, PCollection}
import com.twitter.chill.ClosureCleaner

import scala.reflect.runtime.universe._


class DList[T: TypeTag](val native: PCollection[T],
                        val coderRegistry: CoderRegistry) {
  def applyTransform[U: TypeTag](trans: PTransform[PCollection[T], PCollection[U]])
  : DList[U] = {
    new DList[U](native.apply(trans), this.coderRegistry)
  }

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
}


object DList {
//  implicit def pcollectionToDList[T: ClassTag](p: PCollection[T]): DList[T] = new DList[T](p)
  implicit def dlistToPCollection[T: TypeTag](d: DList[T]): PCollection[T] = d.native

  def clean[F <: AnyRef](f: F): F = {
    ClosureCleaner(f)
    f
  }
}