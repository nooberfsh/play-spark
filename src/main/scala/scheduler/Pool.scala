package scheduler

sealed trait FairComparable[T] {
  def fairCompare(lhs: T, rhs: T): Boolean
}

sealed trait FifoComaprable[T] {
  def fifoCompare(lhs: T, rhs: T): Boolean
}

object TaskSetManagerComparator
    extends FairComparable[TaskSetManager]
    with FifoComaprable[TaskSetManager] {
  override def fairCompare(lhs: TaskSetManager,
                           rhs: TaskSetManager): Boolean = {
    lhs.runningTasks < rhs.runningTasks
  }

  override def fifoCompare(lhs: TaskSetManager,
                           rhs: TaskSetManager): Boolean = {
    val priority1 = lhs.priority
    val priority2 = rhs.priority
    var res = math.signum(priority1 - priority2)
    if (res == 0) {
      val stageId1 = lhs.stageId
      val stageId2 = rhs.stageId
      res = math.signum(stageId1 - stageId2)
    }
    res < 0
  }
}

object PoolComparator extends FairComparable[Pool] {
  override def fairCompare(lhs: Pool, rhs: Pool): Boolean = {
    val minShare1 = lhs.minShare
    val minShare2 = rhs.minShare
    val runningTasks1 = lhs.runningTasks
    val runningTasks2 = rhs.runningTasks
    val s1Needy = runningTasks1 < minShare1
    val s2Needy = runningTasks2 < minShare2
    val minShareRatio1 = runningTasks1.toDouble / math.max(minShare1, 1.0)
    val minShareRatio2 = runningTasks2.toDouble / math.max(minShare2, 1.0)
    val taskToWeightRatio1 = runningTasks1.toDouble / lhs.weight.toDouble
    val taskToWeightRatio2 = runningTasks2.toDouble / rhs.weight.toDouble

    var compare = 0
    if (s1Needy && !s2Needy) {
      return true
    } else if (!s1Needy && s2Needy) {
      return false
    } else if (s1Needy && s2Needy) {
      compare = minShareRatio1.compareTo(minShareRatio2)
    } else {
      compare = taskToWeightRatio1.compareTo(taskToWeightRatio2)
    }
    if (compare < 0) {
      true
    } else if (compare > 0) {
      false
    } else {
      lhs.name < rhs.name
    }
  }
}

class TaskSetManager {
  def runningTasks: Int = ???
  def priority: Int = ???
  def stageId: Int = ???
}

class Pool {
  val weight: Int = ???
  val minShare: Int = ???
  val name: String = ???

  val tasks: Vector[TaskSetManager] = ???

  def runningTasks: Int = tasks.map(_.runningTasks).sum
}

sealed trait RootPool {
  def getSortedTaskSetQueue: Vector[TaskSetManager]
}

class FifoPool extends RootPool {
  val tasks: Vector[TaskSetManager] = ???

  override def getSortedTaskSetQueue: Vector[TaskSetManager] =
    tasks.sortWith(TaskSetManagerComparator.fifoCompare)
}

class FailPool extends RootPool {
  val pools: Vector[Pool] = ???

  override def getSortedTaskSetQueue: Vector[TaskSetManager] =
    pools
      .sortWith(PoolComparator.fairCompare)
      .flatMap(_.tasks.sortWith(TaskSetManagerComparator.fairCompare))
}
