package org.apache.flink.api.scala.typeutils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.flink.api.common.ExecutionConfig
import org.junit.Test
import org.apache.flink.api.scala._
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}

/**
  * Created by tzulitai on 21/06/2018.
  */
class RandomTest {

  @Test
  def test(): Unit = {
    val serializer = createTypeInformation[List[Int]].createSerializer(new ExecutionConfig)

    val baos = new ByteArrayOutputStream()
    val output = new DataOutputViewStreamWrapper(baos)
    serializer.serialize(List(2, 3, 4), output)

    val config = serializer.snapshotConfiguration()

    val baos2 = new ByteArrayOutputStream()
    val output2 = new DataOutputViewStreamWrapper(baos)
    config.write(output2)



    val restoredSerializer =  serializer.snapshotConfiguration().restoreSerializer()
    val bais = new ByteArrayInputStream(baos.toByteArray)
    val input=  new DataInputViewStreamWrapper(bais)
    val des = restoredSerializer.deserialize(input)
  }

}
