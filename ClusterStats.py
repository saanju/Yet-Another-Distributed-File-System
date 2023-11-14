class ClusterStats():
    def get_three_least_utilized_nodes(self, datanode_list):
        min_val1 = min_val2 = min_val3 = 305.00
        least_loaded_node1 = least_loaded_node2 = least_loaded_node3 = ""
