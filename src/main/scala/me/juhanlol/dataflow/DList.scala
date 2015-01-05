package me.juhanlol.dataflow

import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.transforms.{GroupByKey, PTransform, ParDo}
import com.google.cloud.dataflow.sdk.values.{KV, PCollection}
import com.twitter.chill.ClosureCleaner

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
  def clean[F <: AnyRef](f: F): F = {
    ClosureCleaner(f)
    f
  }
}