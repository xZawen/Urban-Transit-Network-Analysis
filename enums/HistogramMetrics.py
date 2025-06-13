from enum import Enum

"""
    Список метрик с названиями для отрисовки в Histogram
"""


class HistogramMetrics(Enum):
    PAGE_RANK = 'page_rank'
    BEETWEENESSENS = 'beetweenessens'
    DEGREE = 'degree'
    LEIDEN_MODULARITY = 'leiden_modularity'
    LOUVAIN_MODULARITY = 'louvain_modularity'
