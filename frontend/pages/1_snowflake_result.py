import streamlit as st
import requests  # For making API calls
import pandas as pd

# FastAPI API endpoint URL (replace with your actual URL)
API_URL = "http://host.docker.internal:8080/snowflake_query"
title_URL = "http://host.docker.internal:8080/snowflake_title_query"

# Available options for level dropdown
year = ["2021", "2022", "2023", "2024"]
level = ["Level I", "Level II", "Level III"]

# Create dropdowns for level and year range
year_selected = st.selectbox("Select year", year)
level_selected = st.selectbox("Select level", level)

# Button to trigger API call
if st.button("Submit"):
    # Prepare data for API call (level and year range)
    data = {'year': year_selected, 'level': level_selected}
    # Make POST request to API endpoint
    st.write(data)
    response = requests.post(API_URL, json=data)
    # Check if request was successful
    if response.status_code == 200:
        # Display the response (assuming JSON format)
        data = response.json()
        column_names = data.get("column_names")
#        data = pd.DataFrame(data)
        dataframe = pd.DataFrame(data["data"], columns=column_names)  # Create DataFrame with column names
        dataframe_subset = dataframe.iloc[:, :3]
        st.write("**Data from Snowflake:**")
        st.dataframe(dataframe_subset)
#        st.write(data)
    else:
        st.error(f"Error: {response.status_code} - {response.text}")

if st.button("View Data"):
    data = {'year': year_selected, 'level': level_selected}
    # Make POST request to API endpoint
    response = requests.post(title_URL, json=data)

        # Check if request was successful
    if response.status_code == 200:
        # Display the response (assuming JSON format)
        data = response.json()
        column_names = data.get("column_names")
#        data = pd.DataFrame(data)
        dataframe = pd.DataFrame(data["data"], columns=column_names)  # Create DataFrame with column names
        st.write("**Data from Snowflake:**")
        st.dataframe(dataframe)
#        st.write(data)
    else:
        st.error(f"Error: {response.status_code} - {response.text}")




