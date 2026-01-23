import math
from pyflink.table.udf import udf
from pyflink.table import DataTypes

@udf(result_type=DataTypes.DOUBLE())
def udf_function(lat1, lon1, lat2, lon2):
    # Check for None values to avoid crashes
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return 0.0
    
    # CAST TO FLOAT: This fixes the 'decimal.Decimal' vs 'float' error
    lat1, lon1 = float(lat1), float(lon1)
    lat2, lon2 = float(lat2), float(lon2)
        
    R = 6371.0 # Earth's radius in km
    
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    
    a = math.sin(dphi / 2)**2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
    
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))
