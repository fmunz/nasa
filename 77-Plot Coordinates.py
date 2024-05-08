# Databricks notebook source
pip install astropy

# COMMAND ----------

# MAGIC %md
# MAGIC ## plotting cordinates

# COMMAND ----------


from astropy.coordinates import get_sun, Galactic
from astropy.time import Time

def get_sun_coordinates():  
    # Get the current time
    now = Time.now()
    
    # Get the current equatorial coordinates of the Sun
    sun_eq = get_sun(now)
    
    # Convert the equatorial coordinates to galactic coordinates
    sun_galactic = sun_eq.transform_to(Galactic)
    
    return sun_galactic


# COMMAND ----------

import matplotlib.pyplot as plt


gal_coords = (282.85, -28.78)
sun = get_sun_coordinates()

# Step 4: Plot the coordinates on a map
plt.scatter(gal_coords[0], gal_coords[1], color='red', label='Target')
plt.scatter(sun.l.deg, sun.b.deg, color='yellow', marker='*', label='Sun', s=300) # Increase the size of the Sun symbol
plt.xlabel('Galactic Longitude')
plt.ylabel('Galactic Latitude')
plt.title('Galactic Coordinates')
plt.grid()
plt.legend()
plt.show()

