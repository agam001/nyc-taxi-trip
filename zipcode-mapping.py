sc.install_pypi_package("pandas==0.25.1") 
from pyspark.sql.types import StructType,StructField,DoubleType,IntegerType
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .getOrCreate()
    
nyc_zip_sp = spark.read.option("header",True).csv("s3://nyc-analysis/zips.csv")
#nyc_zip_sp.show()
df_nyc=nyc_zip_sp.select(nyc_zip_sp['ZIP Codes'].cast("int"), 
                         nyc_zip_sp['lat'].cast("float"),        
                         nyc_zip_sp['long'].cast("float"))
df_nyc = df_nyc.withColumnRenamed("ZIP Codes", "zip")
df_nyc_pd = df_nyc.toPandas()
df= spark.read.option("header",True).csv(f's3://nyc-tlc/trip data/yellow_tripdata_2009-02.csv')
df1=df.select(df['Start_Lat'].cast('float'),df['Start_Lon'].cast('float'))

zips = {float(row[0]):(float(row[1]),float(row[2])) for row in df_nyc_pd.values}

from math import radians, asin, cos, sin
def dist(x):
    """
Replicating the same formula as mentioned in Wiki
    """
    # convert decimal degrees to radians 
    lat1, long1 = radians(x[0]), radians(x[1])
    
    m = 12756 # distance b/w furthest points on Earth
    m_zip = 0
    for z,y in zip(zips.keys(),zips.values()):
        
        lat2, long2 =  radians(y[0]), radians(y[1])
        # haversine formula 
        dlon = long2 - long1 
        dlat = lat2 - lat1 
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a)) 
        # Radius of earth in kilometers is 6371
        dist = 6371* c
        
        if dist < m:
            m = dist
            m_zip = z
        
    return x[0],x[1],float(m_zip)
    
rdd1 = df.rdd.map(dist)

schema = StructType([ StructField('lat',DoubleType(),False),  
                      StructField('lon',DoubleType(),False),  
                      StructField('zip',DoubleType(),False)                    
                    ])
df2 = rdd1.toDF(schema)

df1.write.mode("overwrite").option("header",True).csv('s3://nyc-analysis/zip_feb_2009')


