"""Threshold model for detecting wildfires."""
from .model import (
    ModelFeatures,
    is_cloud_pixel,
    is_night_pixel,
    is_hot_pixel,
    is_water_pixel,
    predict,
)