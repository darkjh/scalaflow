package me.juhanlol.dataflow

import java.lang.reflect.Method
import java.net.URI

import com.google.api.services.bigquery.model.TableRow
import com.google.cloud.dataflow.sdk.coders._
import com.google.cloud.dataflow.sdk.values.{TimestampedValue, KV}
import org.joda.time.Instant
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

  // register common types
  // TODO byte[] coder ???
  registerCoder[Int](classOf[VarIntCoder])
  registerCoder[java.lang.Integer](classOf[VarIntCoder])
  registerCoder[Long](classOf[VarLongCoder])
  registerCoder[java.lang.Long](classOf[VarLongCoder])
  registerCoder[Double](classOf[DoubleCoder])
  registerCoder[java.lang.Double](classOf[DoubleCoder])
  registerCoder[String](classOf[StringUtf8Coder])
  registerCoder[java.lang.String](classOf[StringUtf8Coder])
  registerCoder[Instant](classOf[InstantCoder])
  registerCoder[java.lang.Void](classOf[VoidCoder])
  registerCoder[URI](classOf[URICoder])
  registerCoder[TimestampedValue[_]](classOf[TimestampedValue.TimestampedValueCoder[_]])

  registerCoder[TableRow](classOf[TableRowJsonCoder])

  registerCoder[KV[_, _]](classOf[KvCoder[_, _]])
  registerCoder[java.lang.Iterable[_]](classOf[IterableCoder[_]])
  registerCoder[java.util.List[_]](classOf[ListCoder[_]])

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