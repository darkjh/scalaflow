package com.google.cloud.dataflow.sdk.transforms


abstract class SDoFn[I, O] extends DoFn[I, O] {
  type Context = DoFn[I, O]#Context
  type ProcessContext = DoFn[I, O]#ProcessContext
}