/*
 * LogicalQueryPlanRoot.h
 *
 *  Created on: Nov 13, 2013
 *      Author: wangli
 */

#ifndef LOGICALQUERYPLANROOT_H_
#define LOGICALQUERYPLANROOT_H_
#ifdef DMALLOC
#include "dmalloc.h"
#endif
#include "./LogicalOperator.h"
#include "../common/ids.h"
#include "../BlockStreamIterator/BlockStreamIteratorBase.h"

// namespace claims {
// namespace logical_query_plan {

struct LimitConstraint {
  /* the upper bound on the number of tuples returned*/
  unsigned long returned_tuples_;
  /* the start position of the first returned tuples.
   * Note: the index starts from 0;
   */
  unsigned long start_position_;
  LimitConstraint(unsigned long return_tuples)
      : returned_tuples_(return_tuples),
        start_position_(0) {
  }
  LimitConstraint(unsigned long return_tuples, unsigned long position)
      : returned_tuples_(return_tuples),
        start_position_(position) {
  }
  LimitConstraint() : returned_tuples_(-1), start_position_(0) { }
  /* return if the limit constraint can be omitted.*/
  bool canBeOmitted() const {
    return returned_tuples_ == -1 & start_position_ == 0;
  }
};

/***
 * @brief: top logical operation in logical query plan,
 *          to generate a few top physical operation.
 */
class LogicalQueryPlanRoot : public LogicalOperator {
 public:
  /* three style decide which one the top physical operation is:
   * BlockStreamPrint/BlockStreamPerformanceMonitorTop/BlockStreamResultCollector
   */
  enum OutputFashion {
    PRINT,
    PERFORMANCE,
    RESULTCOLLECTOR
  };
  /**
   * @brief Method description:
   * @param collecter : specify the id of node that return result to client, which is called master
   *        child : the child logical operation of this operation
   *        fashion : decide the top physical operation
   *                  (BlockStreamPrint,BlockStreamPerformanceMonitorTop,BlockStreamResultCollector)
   *                  generated from this logical operation
   *        limit_constraint : apply the necessary info about limit, default value is no limit
   */
  LogicalQueryPlanRoot(NodeID collecter, LogicalOperator* child,
                       const OutputFashion& fashion = PERFORMANCE,
                       LimitConstraint limit_constraint = LimitConstraint());
  virtual ~LogicalQueryPlanRoot();
  Dataflow GetDataflow();
  /**
   * @brief Method description: generate a few physical operations according to this logical operation
   * @param block_size : give the size of block in physical plan
   * @return the total physical plan include the physical plan generated by child operation
   */
  BlockStreamIteratorBase* GetIteratorTree(const unsigned&);

  /**
   * @brief Method description: NOT USED NOW !!!
   *                            get the optimal physical plan
   */
  bool GetOptimalPhysicalPlan(Requirement requirement,
                              PhysicalPlanDescriptor& physical_plan_descriptor,
                              const unsigned & block_size = 4096 * 1024);

  /**
   * @brief Method description: print the limit info and call child to print their logical operation info
   * @param level : specify the level of this operation, the top level is 0. so this class's level is always 0.
   *                level means level*8 space indentation at the begin of line
   * @return  void
   */
  void Print(int level = 0) const;

 private:
  /**
   * @brief Method description: get all attribute name from dataflow
   * @param dataflow :
   * @return  vector of attribute names;
   */
  std::vector<std::string> getAttributeName(const Dataflow& dataflow) const;

 private:
  NodeID collecter_;
  LogicalOperator* child_;
  OutputFashion fashion_;
  LimitConstraint limit_constraint_;
};

//}  // namespace logical_query_plan
//}  // namespace claims

#endif /* LOGICALQUERYPLANROOT_H_ */

