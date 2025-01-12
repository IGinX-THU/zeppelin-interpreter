import traceback
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics import silhouette_score
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
import json
from datetime import datetime
from pymilvus import Collection, connections


class MilvusClient:
    def __init__(self, host, port):
        try:
            print(f"正在连接 Milvus 数据库，地址：{host}:{port}...")
            connections.connect(alias='default', host=host, port=port)
            print("Milvus连接成功")
        except Exception as e:
            print(f"连接失败: {str(e)}")
            print("详细错误信息：")
            traceback.print_exc()
            raise

    def search_by_path(self, path, table_name):
        collection = Collection(name=table_name)
        query = f'path == "{path}"'
        results = collection.query(expr=query, output_fields=["embedding"])
        if results:
            return np.array(results[0]['embedding'])
        else:
            # 如果没有找到匹配的路径，则返回一个空的 numpy 数组
            return np.array([])


class Node:
    def __init__(self, path='', embedding=None):
        self.path = path
        self.embedding = embedding if embedding is not None and embedding.size > 0 else np.array([])


class Aggregator:
    def __init__(self, threshold=0.8, n_clusters=8, pca_components=50, tsne_components=3):
        self.threshold = threshold
        self.n_clusters = n_clusters
        self.pca_components = pca_components
        self.tsne_components = tsne_components

    def compute_similarity_matrix(self, embeddings):
        # embeddings = [node.embedding for node in nodes]
        similarity_matrix = cosine_similarity(embeddings)
        return similarity_matrix

    def calculate_silhouette_score(self, similarity_matrix, labels):
        """
        计算聚类的轮廓系数。
        """
        distance_matrix = 1 - similarity_matrix
        distance_matrix = (distance_matrix + distance_matrix.T) / 2
        np.fill_diagonal(distance_matrix, 0)
        distance_matrix = np.maximum(0, distance_matrix)
        try:
            silhouette_avg = silhouette_score(distance_matrix, labels, metric='precomputed')
            return silhouette_avg
        except ValueError:
            return -1.0

    def calculate_avg_similarity(self, nodes, labels):
        """
        计算聚类内的平均相似度。
        """
        avg_similarity = 0
        total_pairs = 0
        for label in np.unique(labels):  # 遍历每个簇
            indices = np.where(labels == label)[0]
            for i in range(len(indices)):
                for j in range(i + 1, len(indices)):
                    node_i = nodes[indices[i]]
                    node_j = nodes[indices[j]]
                    similarity = cosine_similarity([node_i.embedding], [node_j.embedding])[0][0]
                    avg_similarity += similarity
                    total_pairs += 1
        if total_pairs > 0:
            return avg_similarity / total_pairs
        return 0

    def calculate_cluster_separation(self, similarity_matrix, labels):
        cluster_separation = 0
        unique_labels = np.unique(labels)
        num_clusters = len(unique_labels)
        for i in range(num_clusters):
            for j in range(i + 1, num_clusters):
                cluster_i = np.where(labels == unique_labels[i])[0]
                cluster_j = np.where(labels == unique_labels[j])[0]
                # 类间的平均距离
                inter_cluster_distance = np.mean([1 - similarity_matrix[x][y] for x in cluster_i for y in cluster_j])
                cluster_separation += inter_cluster_distance
        return cluster_separation / (num_clusters * (num_clusters - 1) / 2 if num_clusters > 1 else 1)

    def find_optimal_clusters(self, similarity_matrix, max_clusters=5):
        """
        自动选择最优的聚类数，根据轮廓系数。
        返回最优聚类数、对应的 labels 和最大轮廓系数。
        """
        best_score = -1
        best_n_clusters = self.n_clusters
        best_labels = None

        for n_clusters in range(3, max_clusters + 1):
            kmeans = KMeans(n_clusters=n_clusters, random_state=0)
            kmeans.fit(similarity_matrix)
            labels = kmeans.labels_
            silhouette_avg = self.calculate_silhouette_score(similarity_matrix, labels)
            print(f"聚类数为：{n_clusters}  轮廓系数为：{silhouette_avg}")

            if silhouette_avg > best_score:
                best_score = silhouette_avg
                best_n_clusters = n_clusters
                best_labels = labels

        return best_n_clusters, best_labels, best_score

    def apply_pca(self, embeddings, n_samples):
        n_components = min(self.pca_components, n_samples)
        pca = PCA(n_components=n_components)
        pca_embeddings = pca.fit_transform(embeddings)
        return pca_embeddings

    def apply_tsne(self, embeddings, n_samples):
        perplexity = min(30, n_samples - 1)
        tsne = TSNE(n_components=self.tsne_components, perplexity=perplexity)
        tsne_embeddings = tsne.fit_transform(embeddings)
        return tsne_embeddings

    # 聚合
    def aggregate(self, root_nodes):
        embeddings = [node.embedding for node in root_nodes]
        n_samples = len(root_nodes)

        # 先进行 PCA 降维
        pca_embeddings = self.apply_pca(embeddings, n_samples)
        print(f"PCA 降维后的形状：{pca_embeddings.shape}")

        # 再进行 t-SNE 降维
        tsne_embeddings = self.apply_tsne(pca_embeddings, n_samples)
        print(f"t-SNE 降维后的形状：{tsne_embeddings.shape}")

        similarity_matrix = self.compute_similarity_matrix(tsne_embeddings)
        print(f"相似度矩阵为：{similarity_matrix}")
        print(f"原先森林数为: {len(root_nodes)}")

        optimal_n_clusters, labels, silhouette_avg = self.find_optimal_clusters(
            similarity_matrix=similarity_matrix,
            max_clusters=max(int(len(root_nodes) / 2), 3)
        )
        print(f"最优聚类数: {optimal_n_clusters}")
        print(f"轮廓系数: {silhouette_avg}")

        avg_similarity = self.calculate_avg_similarity(root_nodes, labels)
        cluster_separation = self.calculate_cluster_separation(similarity_matrix, labels)

        buckets = {}
        for idx, label in enumerate(labels):
            if label not in buckets:
                buckets[label] = []
            buckets[label].append(root_nodes[idx])

        node_components = list(buckets.values())
        print(f"分块结果：{[[node.path for node in component] for component in node_components]}")

        return silhouette_avg, avg_similarity, cluster_separation, labels


class UDFMerge:
    def __init__(self):
        self.milvus_host = 'localhost'
        self.milvus_port = 19530
        self.table_name = 'data_embedding'

    def transform(self, data, args, kvargs):
        print("enter transform Merge success")
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        # print(kvargs)
        path_list = json.loads(kvargs["str"].decode("utf-8"))
        milvus_client = MilvusClient(self.milvus_host, self.milvus_port)

        nodes = []
        for path in path_list:
            # 查询 Milvus 数据库获取路径对应的 embedding
            embedding = milvus_client.search_by_path(path, self.table_name)
            if embedding is None or len(embedding) == 0:
                error_message = f"Error: No embedding found for path: {path}"
                print(error_message)
                raise ValueError(error_message)  # 抛出异常并输出错误原因
            node = Node(path=path, embedding=embedding)
            nodes.append(node)

        # for node in nodes:
        #     print(node.path)
        #     print(node.embedding)

        aggregator = Aggregator()
        silhouette_avg, avg_similarity, cluster_separation, labels = aggregator.aggregate(nodes)
        print("finish aggregate")
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print(f"silhouette_avg为: {silhouette_avg}\navg_similarity为: {avg_similarity}\ncluster_separation为: {cluster_separation}")

        # 生成最终的结果
        result = self.generate_result(nodes, labels)
        print("finish generate result")
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return result

    def generate_result(self, root_nodes, labels):
        result = [['(name)', '(label)'], ['BINARY', 'BINARY']]
        for idx, node in enumerate(root_nodes):
            result.append([node.path.encode('utf-8'), str(labels[idx]).encode('utf-8')])
        return result


# # 测试数据
# kvargs = {'str': b'["\xe5\xae\x9e\xe5\x86\xb5\xe5\x88\x86\xe6\x9e\x90\xe6\x95\xb0\xe6\x8d\xae","\xe6\xb5\xb7\xe5\xba\x95\xe5\x9c\xb0\xe5\xbd\xa2","\xe6\xb5\xb7\xe6\xb4\x8b\xe5\xba\x95\xe8\xb4\xa8","\xe5\x9b\xbd\xe5\xae\xb6\xe6\xb7\xb1\xe6\xb5\xb7\xe5\x9f\xba\xe5\x9c\xb0\xe7\xae\xa1\xe7\x90\x86\xe4\xb8\xad\xe5\xbf\x83","\xe4\xb8\x9c\xe6\xb5\xb7\xe4\xbf\xa1\xe6\x81\xaf\xe4\xb8\xad\xe5\xbf\x83","\xe9\x87\x8d\xe7\x82\xb9\xe7\xa0\x94\xe5\x8f\x91\xe8\xae\xa1\xe5\x88\x92","\xe5\x8c\x97\xe5\x86\xb0\xe6\xb4\x8b\xe5\x8d\xab\xe6\x98\x9f\xe9\x81\xa5\xe6\x84\x9f\xe4\xba\xa7\xe5\x93\x81\xe6\x95\xb0\xe6\x8d\xae","\xe6\xb5\xb7\xe6\xb4\x8b\xe6\xb0\x94\xe8\xb1\xa1","\xe5\x8c\x97\xe6\xb5\xb7\xe4\xbf\xa1\xe6\x81\xaf\xe4\xb8\xad\xe5\xbf\x83","\xe6\xb5\xb7\xe6\xb4\x8b\xe5\x9c\xb0\xe7\x90\x83\xe7\x89\xa9\xe7\x90\x86","\xe9\xa3\x8e\xe7\x94\xb5\xe9\x81\xa5\xe6\x84\x9f\xe4\xba\xa7\xe5\x93\x81\xe6\x95\xb0\xe6\x8d\xae","\xe5\x9b\xbd\xe5\xae\xb6\xe5\x8d\xab\xe6\x98\x9f\xe6\xb5\xb7\xe6\xb4\x8b\xe5\xba\x94\xe7\x94\xa8\xe4\xb8\xad\xe5\xbf\x83","\xe7\xbb\x9f\xe8\xae\xa1\xe5\x88\x86\xe6\x9e\x90\xe6\x95\xb0\xe6\x8d\xae","\xe6\xb5\xb7\xe6\xb4\x8b\xe6\xb0\xb4\xe6\x96\x87","\xe6\xb5\xb7\xe6\xb4\x8b\xe7\x94\x9f\xe7\x89\xa9","\xe5\x86\x8d\xe5\x88\x86\xe6\x9e\x90\xe6\x95\xb0\xe6\x8d\xae","\xe7\x9f\xa2\xe9\x87\x8f\xe5\x9c\xb0\xe5\x9b\xbe\xe6\x95\xb0\xe6\x8d\xae","\xe4\xb8\xad\xe5\x9b\xbd\xe8\xbf\x91\xe6\xb5\xb7\xe7\x8e\xaf\xe5\xa2\x83\xe9\x81\xa5\xe6\x84\x9f\xe4\xba\xa7\xe5\x93\x81\xe6\x95\xb0\xe6\x8d\xae","\xe5\xbd\xb1\xe5\x83\x8f\xe9\x81\xa5\xe6\x84\x9f","\xe7\xa7\x91\xe6\x8a\x80\xe5\x9f\xba\xe7\xa1\x80\xe8\xb5\x84\xe6\xba\x90\xe8\xb0\x83\xe6\x9f\xa5\xe4\xb8\x93\xe9\xa1\xb9"]'}
# # 执行
# udf = UDFMerge()
# result = udf.transform(None, None, kvargs)
# for row in result:
#     decoded_row = [col.decode('utf-8') if isinstance(col, bytes) else col for col in row]
#     print(decoded_row)
