package com.google.cloud.dataflow.sdk.transforms

import scala.reflect.ClassTag
import com.twitter.chill.ClosureCleaner

/**
 * Created by darkjh on 12/30/14.
 */
abstract class DList[T: ClassTag](var dep: DList[_]) {
  def map[U: ClassTag](f: T => U): DList[U] = new MappedDList(this, DList.clean(f))

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): DList[U] =
    new FlatMappedDList(this, DList.clean(f))

  def filter(f: T => Boolean): DList[T] = new FilteredDList(this, DList.clean(f))
}

object DList {
  def clean[F <: AnyRef](f: F): F = {
    ClosureCleaner(f)
    f
  }
}

class TextDList(pattern: String, name: Option[String] = None)
  extends DList[String](null)

class MappedDList[O: ClassTag, I: ClassTag](prev: DList[I], f: I => O)
  extends DList[O](prev)

class FlatMappedDList[U: ClassTag, T: ClassTag]
(prev: DList[T], f: T => TraversableOnce[U])
  extends DList[U](prev)

class FilteredDList[T: ClassTag](prev: DList[T], f: T => Boolean)
  extends DList[T](prev)