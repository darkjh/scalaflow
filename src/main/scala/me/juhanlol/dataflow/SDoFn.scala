package me.juhanlol.dataflow

import com.google.cloud.dataflow.sdk.transforms.DoFn

abstract class SDoFn[I, O] extends DoFn[I, O] {
  type Context = DoFn[I, O]#Context
  type ProcessContext = DoFn[I, O]#ProcessContext
}