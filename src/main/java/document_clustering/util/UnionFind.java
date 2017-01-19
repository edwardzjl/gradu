package document_clustering.util;

/**
 * Created by edwardlol on 2016/12/9.
 */
public class UnionFind {

    /**
     * access to component id (site indexed)
     */
    private int[] id;

    private int[] rank;

    /**
     * number of components
     */
    private int count;

    public UnionFind(int N) {
        // Initialize component id array.
        count = N;
        id = new int[N];
        for (int i = 0; i < N; i++)
            id[i] = i;
        rank = new int[N];
        for (int i = 0; i < N; i++)
            rank[i] = 1;    // 初始情况下，每个组的大小都是1
    }

    public int getCount() {
        return count;
    }

    public boolean connected(int p, int q) {
        return find(p) == find(q);
    }

    public int find(int p) {
        while (p != id[p]) {
            // 将p节点的父节点设置为它的爷爷节点
            id[p] = id[id[p]];
            p = id[p];
        }
        return p;
    }

    public boolean union(int p, int q) {
        int i = find(p);
        int j = find(q);
        if (i == j) {
            return false;
        }

        // 将小树作为大树的子树
        if (rank[i] < rank[j]) {
            id[i] = j;
//            rank[j] += rank[i];
        } else {
            id[j] = i;
            if (rank[i] == rank[j]) {
                rank[j]++;
            }
//            rank[i] += rank[j];
        }
        count--;
        return true;
    }

    public void printRoots() {
        for (int id : this.id) {
            System.out.println(find(id));
        }
    }

    public int[] getId() {
        return id;
    }
}

// End UnionFind.java
