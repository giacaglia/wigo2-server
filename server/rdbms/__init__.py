from __future__ import absolute_import


from playhouse.dataset import DataSet
from config import Configuration


rdbms = DataSet(Configuration.DATABASE_URL)