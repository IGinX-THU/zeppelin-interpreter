package org.apache.zeppelin.iginx.util.algorithm.mergeforest;

import org.apache.zeppelin.iginx.util.TreeNode;

public interface MergeForestStrategy {
  void mergeForest(TreeNode root) throws Exception;
}
