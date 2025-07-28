from pyspark.sql.functions import *
from pyspark.sql.types import *
@udf
def addone(n,returnType=DecimalType(13,2)):
    if n is not None:
       n=n+1
    return n