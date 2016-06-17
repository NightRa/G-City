package univ.bigdata.course.part2

import org.junit.{Assert, Test}
import univ.bigdata.course.SparkMain
import univ.bigdata.course.part3.ExecuteMap.Rank
import univ.bigdata.course.part3.Map
import univ.bigdata.course.TestBuilders.Epsilon
class MeanAveragePrecisionTests {

  def asRDD (ranks : Vector[Rank]*) = {
    SparkMain.sc.parallelize(ranks)
  }

  @Test
  def test1 : Unit = {
    val rankVector1 = Vector(0,1,2)
    val expectedMapValue = 1.0
    val actualMapValue =  Map.calcMap(asRDD(rankVector1))
    Assert.assertEquals(expectedMapValue, actualMapValue, Epsilon)
  }

  @Test
  def test2 : Unit = {
    val rankVector1 = Vector(0,1,2)
    val rankVector2 = Vector(0,1,2,3,4,5,6,7,8,9,10,11,12)
    val rankVector3 = Vector(0,1,2,3,4,5,6)
    val expectedMapValue = 1.0
    val actualMapValue =  Map.calcMap(asRDD(rankVector1, rankVector2, rankVector3))
    Assert.assertEquals(expectedMapValue, actualMapValue, Epsilon)
  }

  @Test
  def test3 : Unit = {
    val rankVector1 = Vector()
    val rankVector2 = Vector(0,1,2,3,4,5,6,7,8,9,10,11,12)
    val rankVector3 = Vector()
    val expectedMapValue = 1.0
    val actualMapValue =  Map.calcMap(asRDD(rankVector1, rankVector2, rankVector3))
    Assert.assertEquals(expectedMapValue, actualMapValue, Epsilon)
  }
  @Test
  def test4 : Unit = {
    val rankVector1 = Vector()
    val rankVector2 = Vector()
    val expectedMapValue = 0.0
    val actualMapValue =  Map.calcMap(asRDD(rankVector1, rankVector2))
    Assert.assertEquals(expectedMapValue, actualMapValue, Epsilon)
  }
}
