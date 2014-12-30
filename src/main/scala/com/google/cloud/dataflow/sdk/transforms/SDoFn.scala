package com.google.cloud.dataflow.sdk.transforms

import com.google.common.reflect.TypeToken

import scala.reflect.ClassTag

abstract class SDoFn[I: ClassTag, O: ClassTag] extends DoFn[I, O] {
  type Context = DoFn[I, O]#Context
  type ProcessContext = DoFn[I, O]#ProcessContext


  override def getInputTypeToken: TypeToken[I] = {
    val clazz = implicitly[ClassTag[I]].runtimeClass
    TypeToken.of(clazz).asInstanceOf[TypeToken[I]]
  }

  override def getOutputTypeToken: TypeToken[O] = {
    val clazz = implicitly[ClassTag[O]].runtimeClass
    TypeToken.of(clazz).asInstanceOf[TypeToken[O]]
  }
}