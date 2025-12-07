"""
This program calls the WeatherAPI.com to retrieve current and
forecast weather information for a specific geolocation.

It is structured to be runnable as a standalone script or as a tool
within a larger system, following a similar pattern to the example provided.
"""
import json
import requests
import argparse
from typing import Type
from pydantic import BaseModel, Field, validator


# Pydantic models to define and validate user and tool parameters.
class UserParameters(BaseModel):
    """Configuration parameters for the WeatherAPI.com."""
    weatherapi_api_key: str = Field(description="Your WeatherAPI.com API key.")


class ToolParameters(BaseModel):
    """Input parameters for the tool call."""
    latitude: float = Field(
        description="The latitude of the location (e.g., 51.5074)."
    )
    longitude: float = Field(
        description="The longitude of the location (e.g., -0.1278)."
    )
    
def run_tool(
    config: UserParameters,
    args: ToolParameters,
):
    """
    Fetches weather data from the WeatherAPI.com for the specified geolocation.
    
    Args:
        config (UserParameters): The API configuration, including the API key.
        args (ToolParameters): The geolocation details (latitude and longitude).
    
    Returns:
        str: A JSON string of the raw weather data or an error message.
    """
    # Base URL for the WeatherAPI.com forecast API endpoint
    base_url = "http://api.weatherapi.com/v1/forecast.json"
    
    # Construct the API call URL with the geolocation and API key
    params = {
        "key": config.weatherapi_api_key,
        "q": f"{args.latitude},{args.longitude}",  # WeatherAPI uses a single 'q' parameter for lat/lon
        "days": 3  # Retrieve current and a 3-day forecast
    }
    
    try:
        # Make the GET request to the WeatherAPI.com
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        
        # Return the JSON response as a formatted string
        return json.dumps(response.json(), indent=2)
        
    except requests.exceptions.HTTPError as http_err:
        return f"HTTP error occurred: {http_err} - Check your API key or the location."
    except Exception as err:
        return f"An error occurred: {err}"


# --- Script Execution ---
# This part of the code allows the program to be executed from the command line.
OUTPUT_KEY = "tool_output"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Call the WeatherAPI.com to get weather data."
    )
    
    # Define arguments for the script
    parser.add_argument(
        "--user-params", 
        required=True,
        help="JSON string for user configuration parameters (e.g., {'weatherapi_api_key': 'your_key'})"
    )
    
    parser.add_argument(
        "--tool-params",
        required=True,
        help="JSON string for tool arguments (e.g., {'latitude': 51.5, 'longitude': -0.1})"
    )
    
    args = parser.parse_args()
    
    # Parse JSON strings into Python dictionaries
    try:
        config_dict = json.loads(args.user_params)
        params_dict = json.loads(args.tool_params)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        exit(1)
    
    # Validate the dictionaries against Pydantic models
    try:
        config = UserParameters(**config_dict)
        params = ToolParameters(**params_dict)
    except Exception as e:
        print(f"Error validating parameters: {e}")
        exit(1)

    # Call the main function and print the output
    output = run_tool(
        config,
        params
    )
    print(OUTPUT_KEY, output)
