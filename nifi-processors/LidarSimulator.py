import numpy as np
import laspy
from pyproj import CRS

def create_vessel_geo(center_lon, center_lat, length_m, width_m, height_m, num_points, shape_type="box"):
    # Convert meters to approximate degree offsets (at 59 deg N)
    # 1 deg lat approx 111,000m; 1 deg lon approx 57,000m
    lat_scale = 1/111000
    lon_scale = 1/57000

    if shape_type == "submarine":
        z = np.random.uniform(-height_m/2, height_m/2, num_points)
        angle = np.random.uniform(0, 2 * np.pi, num_points)
        lon = center_lon + (width_m/2 * np.cos(angle)) * lon_scale
        lat = center_lat + np.random.uniform(-length_m/2, length_m/2, num_points) * lat_scale
    elif shape_type == "sailing":
        lat = center_lat + np.random.uniform(-length_m/2, length_m/2, num_points) * lat_scale
        lon = center_lon + np.random.uniform(-width_m/2, width_m/2, num_points) * lon_scale
        z = np.random.uniform(0, height_m/4, num_points)
        # Mast
        mast_z = np.random.uniform(0, height_m, 500)
        return np.column_stack((np.full(num_points+500, center_lon), 
                                np.full(num_points+500, center_lat), 
                                np.concatenate([z, mast_z])))
    else:
        lon = center_lon + np.random.uniform(-width_m/2, width_m/2, num_points) * lon_scale
        lat = center_lat + np.random.uniform(-length_m/2, length_m/2, num_points) * lat_scale
        z = np.random.uniform(0, height_m, num_points)
    
    return np.column_stack((lon, lat, z))

def generate_baltic_lidar(filename="baltic_fleet.las"):
    # Baltic Sea Location: Near Stockholm/Gotland area
    base_lon, base_lat = 19.0, 59.0 
    
    # (Lon, Lat, L, W, H, points, style)
    vessels_data = [
        (base_lon, base_lat, 30, 8, 5, 8000, "box"),            # Tanker
        (base_lon + 0.001, base_lat, 20, 7, 8, 10000, "box"),   # Container
        (base_lon + 0.002, base_lat, 15, 6, 6, 6000, "box"),    # Ferry
        (base_lon + 0.003, base_lat, 10, 3, 12, 3000, "sailing"),# Sailing Boat
        (base_lon + 0.004, base_lat, 25, 5, 5, 7000, "submarine")# Submarine
    ]
    
    all_points = np.vstack([create_vessel_geo(*v) for v in vessels_data])
    
    # Create Header with WGS84 (EPSG:4326)
    header = laspy.LasHeader(point_format=3, version="1.4")
    header.add_crs(CRS.from_epsg(4326))
    
    # Crucial: Scales must be very small for Lon/Lat degrees (0.0000001)
    header.scales = [1e-7, 1e-7, 0.01]
    header.offsets = [base_lon, base_lat, 0]
    
    las = laspy.LasData(header)
    las.x, las.y, las.z = all_points[:, 0], all_points[:, 1], all_points[:, 2]
    las.intensity = np.random.randint(50, 255, len(all_points))
    
    las.write(filename)
    print(f"Generated Baltic Sea LiDAR file at {base_lat}N, {base_lon}E")

generate_baltic_lidar()
