from abc import abstractmethod

from context import DBGraphParameters
from database.Neo4jConnection import Neo4jConnection
"""
    Класс содержащий query для вычисления распределения метрик сети 
"""



class MetricsDistributionNode:
    def __init__(self, db_params: DBGraphParameters):
        self.node_name = db_params.main_node_name
        self.rels_name = db_params.main_rels_name
        self.node_geometry_identity = db_params.node_geometry_identity
        self.metrics_calculate = self.metrics_calculate()
        self.connection = Neo4jConnection()

    @abstractmethod
    def metrics_calculate(self):
        pass


    def calculate_distribution(self, needLog = False):
        query = f'''
            MATCH (first_node:{self.node_name})
            WITH first_node, {self.metrics_calculate} AS Metric
            RETURN first_node.{self.node_geometry_identity} AS NodeIdentity, Metric
            ORDER BY Metric DESC
        '''
        return self.connection.execute_query(query, needLog).records

class DegreeDistribution(MetricsDistributionNode):
    def metrics_calculate(self):
        return 'count(rels)'

    def calculate_distribution(self, needLog = False):
        query = f'''
            MATCH (first_node:{self.node_name})-[rels:{self.rels_name}]-(second_node:{self.node_name})
            WITH first_node, {self.metrics_calculate} AS Metric
            RETURN first_node.{self.node_geometry_identity} AS NodeIdentity, Metric
            ORDER BY Metric DESC
        '''
        return self.connection.execute_query(query, needLog).records

class PageRankDistribution(MetricsDistributionNode):
    def metrics_calculate(self):
        return 'first_node.pageRank'

class BetweennessDistribution(MetricsDistributionNode):
    def metrics_calculate(self):
        return 'first_node.betweenness'

class LouvainClusteringDistribution(MetricsDistributionNode):
    def metrics_calculate(self):
        return 'first_node.louvain_community'

class LeidenClusteringDistribution(MetricsDistributionNode):
    def metrics_calculate(self):
        return 'first_node.leiden_community'
