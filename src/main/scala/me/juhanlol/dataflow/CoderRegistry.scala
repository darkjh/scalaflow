package me.juhanlol.dataflow

import java.lang.reflect.Method

import com.google.cloud.dataflow.sdk.coders._
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

  def registerCoder[T: TypeTag](coderClazz: Class[_]): Unit = {
    val (rawType, typeArgs) = TypeResolver.resolve[T]()
    val numTypeArgs = typeArgs.length

    val factoryMethodArgTypes = Array.fill(numTypeArgs)(classOf[Coder[_]])
    // TODO error check
    val factoryMethod = coderClazz.getDeclaredMethod(
      "of", factoryMethodArgTypes:_*)

    coderMap.put(rawType, CoderFactory.of(coderClazz, factoryMethod))
  }

  def getDefaultCoder[T: TypeTag]: Coder[T] = {
    // simple case, exact type, no handling for specialization
    val (rawType, typeArgs) = TypeResolver.resolve[T]()
    coderMap(rawType).create(Nil).asInstanceOf[Coder[T]]
  }
}
