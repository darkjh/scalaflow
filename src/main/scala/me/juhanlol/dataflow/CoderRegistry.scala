package me.juhanlol.dataflow

import java.lang.reflect.Method

import com.google.cloud.dataflow.sdk.coders._
import com.google.cloud.dataflow.sdk.values.KV
import scala.collection.mutable
import scala.reflect.runtime.universe._

trait CoderFactory {
  def create[_](typeArgumentCoders: List[Coder[_]]): Coder[_]
}

object CoderFactory {
  def of(clazz: Class[_], method: Method): CoderFactory = {
    new CoderFactory {
      override def create[_](coders: List[Coder[_]]): Coder[_] = {
        // TODO catch errors
        method.invoke(null, coders.toArray:_*).asInstanceOf[Coder[_]]
      }
    }
  }
}

class CoderRegistry {
  val coderMap = mutable.Map[Type, CoderFactory]()

  registerCoder[Int](classOf[VarIntCoder])
  registerCoder[Integer](classOf[VarIntCoder])
  registerCoder[String](classOf[StringUtf8Coder])
  registerCoder[java.lang.String](classOf[StringUtf8Coder])

  registerCoder[KV[_, _]](classOf[KvCoder[_, _]])

  def registerCoder[T: TypeTag](coderClazz: Class[_]): Unit = {
    val resolvedInfo = TypeResolver.resolve[T]()
    val numTypeArgs = resolvedInfo.numArgs

    val factoryMethodArgTypes = Array.fill(numTypeArgs)(classOf[Coder[_]])

    // TODO error check
    val factoryMethod = coderClazz.getDeclaredMethod(
      "of", factoryMethodArgTypes:_*)

    coderMap.put(resolvedInfo.raw, CoderFactory.of(coderClazz, factoryMethod))
  }

  def getDefaultCoder[T: TypeTag]: Coder[T] = {
    // TODO support arbitrary level of type arguments
    val resolvedInfo = TypeResolver.resolve[T]()
    val typeArgsCoders = resolvedInfo.actualTypeArgs.map(
      t => coderMap(t.erasure).create(Nil))
    coderMap(resolvedInfo.raw).create(typeArgsCoders).asInstanceOf[Coder[T]]
  }
}