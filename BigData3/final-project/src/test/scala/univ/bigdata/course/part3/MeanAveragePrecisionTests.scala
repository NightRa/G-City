package univ.bigdata.course.part3

import org.junit.{Assert, Test}
import univ.bigdata.course.part3.ExecuteMap.Rank
import univ.bigdata.course.TestBuilders.Epsilon
class MeanAveragePrecisionTests {

  def asIterator(ranks : Vector[Rank]*) : Iterator[Array[Rank]] = {
    ranks.map(_.toArray)
          .iterator
  }

  @Test
  def test1 : Unit = {
    val rankVector1 = Vector(1,2).map(_-1)
    val expectedMapValue = 1.0
    val actualMapValue = Map.calcMap(asIterator(rankVector1))
    Assert.assertEquals(expectedMapValue, actualMapValue, Epsilon)
  }

  @Test
  def test2 : Unit = {
    val rankVector1 = Vector(1,2,3).map(_-1)
    val rankVector2 = Vector(1,2,3,4,5,6,7,8,9,10,11,12).map(_-1)
    val rankVector3 = Vector(1,2,3,4,5,6).map(_-1)
    val expectedMapValue = 1.0
    val actualMapValue =  Map.calcMap(asIterator(rankVector1, rankVector2, rankVector3))
    Assert.assertEquals(expectedMapValue, actualMapValue, Epsilon)
  }

  @Test
  def test3 : Unit = {
    val rankVector1 = Vector()
    val rankVector2 = Vector(1,2,3,4,5,6,7,8,9,10,11,12).map(_-1)
    val rankVector3 = Vector()
    val expectedMapValue = 1.0
    val actualMapValue =  Map.calcMap(asIterator(rankVector1, rankVector2, rankVector3))
    Assert.assertEquals(expectedMapValue, actualMapValue, Epsilon)
  }
  @Test
  def test4 : Unit = {
    val rankVector1 = Vector()
    val rankVector2 = Vector()
    val expectedMapValue = 0.0
    val actualMapValue =  Map.calcMap(asIterator(rankVector1, rankVector2))
    Assert.assertEquals(expectedMapValue, actualMapValue, Epsilon)
  }
  @Test
  def test5 : Unit = {
    val rankVector1 = Vector(1,3,5,6,7,8,9,13).map(_-1)  // (1/1 + 2/3 + 3/5 + 4/6 + 5/7 + 6/8 + 7/9 + 8/13) / 8
    val rankVector2 = Vector()
    val rankVector3 = Vector(2,5,7,9,100,102).map(_-1)   // (1/2 + 2/5 + 3/7 + 4/9 + 5/100 +6/102) / 6
    val rankVector4 = Vector()
    val rankVector5 = Vector(4,6,8,9,12,14,22).map(_-1)  // (1/4 + 2/6 + 3/8 + 4/9 + 5/12 + 6/14 + 7/22) / 7
    val expectedMapValue = (0.72384768009 + 0.3136399005 + 0.36659967017) / 3.0
    val actualMapValue =  Map.calcMap(asIterator(rankVector1, rankVector2, rankVector3, rankVector4, rankVector5))
    Assert.assertEquals(expectedMapValue, actualMapValue, Epsilon)
  }

  @Test
  def test6 : Unit = {
    val rankVector1 = Vector()
    val rankVector2 = Vector(1,3,4,5,6,7).map(_-1) // (1/1 + 2/3 + 3/4 + 4/5 + 5/6 + 6/7)/6
    val rankVector3 = Vector()
    val rankVector4 = Vector()
    val rankVector5 = Vector()
    val rankVector6 = Vector()
    val rankVector7 = Vector(1).map(_-1) // 1/1
    val rankVector8 = Vector()
    val expectedMapValue = (0.817857143 + 1.0) / 2.0
    val actualMapValue =  Map.calcMap(asIterator(rankVector1, rankVector2, rankVector3, rankVector4, rankVector5, rankVector6, rankVector7, rankVector8))
    Assert.assertEquals(expectedMapValue, actualMapValue, Epsilon)
  }

  @Test
  def test7 : Unit = {
    val expectedMapValue = 0.0
    val actualMapValue =  Map.calcMap(asIterator())
    Assert.assertEquals(expectedMapValue, actualMapValue, Epsilon)
  }

  @Test
  def test8 : Unit = {
    val rankVector1 = Vector(1, 1000000000).map(_-1)  // (1/1 + 2/1000000000)
    val expectedMapValue = 0.500000001
    val actualMapValue =  Map.calcMap(asIterator(rankVector1))
    Assert.assertEquals(expectedMapValue, actualMapValue, Epsilon)
  }
}
