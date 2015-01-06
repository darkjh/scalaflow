package me.juhanlol.dataflow

import com.google.cloud.dataflow.sdk.testing.TestPipeline

case class TestJob extends DataflowJob(TestPipeline.create())