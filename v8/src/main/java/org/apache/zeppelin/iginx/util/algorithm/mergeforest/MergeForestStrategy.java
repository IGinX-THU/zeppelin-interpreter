package org.apache.zeppelin.iginx.util.algorithm.mergeforest;

import org.apache.zeppelin.iginx.util.TreeNode;

public interface MergeForestStrategy {
  /**
   * 合并森林(root下的 children)算法，若初始有 n 棵树，则最终合并成 sqrt(n) 棵树
   *
   * @param root
   * @throws Exception
   */
  void mergeForest(TreeNode root) throws Exception;
}
