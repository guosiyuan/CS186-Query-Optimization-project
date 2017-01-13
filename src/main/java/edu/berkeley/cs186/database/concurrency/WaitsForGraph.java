package edu.berkeley.cs186.database.concurrency;

import java.util.*;

/**
 * A waits for graph for the lock manager (used to detect if
 * deadlock will occur and throw a DeadlockException if it does).
 */
public class WaitsForGraph {

  // We store the directed graph as an adjacency list where each node (transaction) is
  // mapped to a list of the nodes it has an edge to.
  private Map<Long, ArrayList<Long>> graph;

  public WaitsForGraph() {
    graph = new HashMap<Long, ArrayList<Long>>();
  }

  public boolean containsNode(long transNum) {
    return graph.containsKey(transNum);
  }

  protected void addNode(long transNum) {
    if (!graph.containsKey(transNum)) {
      graph.put(transNum, new ArrayList<Long>());
    }
  }

  protected void addEdge(long from, long to) {
    if (!this.edgeExists(from, to)) {
      ArrayList<Long> edges = graph.get(from);
      edges.add(to);
    }
  }

  protected void removeEdge(long from, long to) {
    if (this.edgeExists(from, to)) {
      ArrayList<Long> edges = graph.get(from);
      edges.remove(to);
    }
  }

  protected boolean edgeExists(long from, long to) {
    if (!graph.containsKey(from)) {
      return false;
    }
    ArrayList<Long> edges = graph.get(from);
    return edges.contains(to);
  }

  /**
   * Checks if adding the edge specified by to and from would cause a cycle in this
   * WaitsForGraph. Does not actually modify the graph in any way.
   * @param from the transNum from which the edge points
   * @param to the transNum to which the edge points
   * @return
   */
  protected boolean edgeCausesCycle(long from, long to) {
    //TODO: Implement Me!!
    HashSet<Long> marked = new HashSet<Long>();//store transaction that we have seen
    boolean[] hasCycle = {false};
    findCycle(marked, to, from, hasCycle);//starts at to, ends at front, thenadding from to to will cause a cycle
    return hasCycle[0];
  }

  private void findCycle(HashSet<Long> marked, long from, long to, boolean[] hasCycle) {
    if (hasCycle[0] == true) {//already detected cycle
      return;
    }

    for (Long eDge : graph.get(from)) {//branches
      if (eDge == to) {
        hasCycle[0] = true;
        return;
      } else {
        findCycle(marked,eDge,to, hasCycle);
      }
      // if(!marked.contains(eDge)) {//new node
      //     marked.add(eDge);
      //     findCycle(marked,eDge,to, hasCycle);
      // } else if (eDge == to) {//reach destination, has cycle
      //     hasCycle[0] = true;
      //     return;
      // }
    }
    return;

}







}
