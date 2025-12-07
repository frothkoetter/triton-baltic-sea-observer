import cml.data_v1 as cmldata
from openai import OpenAI
import json
import pandas as pd
from datetime import datetime
from io import StringIO 
import sys # For error logging

# --- Database and Connection Configuration ---
CONNECTION_NAME = "cdw-aw-se-impala"
# NOTE: API_KEY is set in the notebook environment, ensuring compatibility.
API_KEY = json.load(open("/tmp/jwt"))["access_token"]

# --- LLM Configuration ---
MODEL_ID = "mistralai/mistral-7b-instruct-v0.3"
BASE_URL = "https://ml-641a1b1b-617.se-sandb.a465-9q4k.cloudera.site/namespaces/serving-default/endpoints/mistral7binstruct/v1"
# --- Function Definitions ---

def generate_summary_sql(summary_text, timestamp):
    """
    Generates a single, bulk SQL INSERT statement for the 
    Situation_Awareness_Summary table, separating value sets by comma.
    """
    values = []
    # Format the timestamp for SQL (YYYY-MM-DD HH:MM:SS)
    sql_timestamp = timestamp.strftime("'%Y-%m-%d %H:%M:%S'")
    
    # Split the summary into lines for separate row insertion
    for line_num, line_content in enumerate(summary_text.splitlines()):
        # Skip empty or whitespace-only lines
        if not line_content.strip():
            continue
            
        # Escape single quotes within the content for SQL safety
        escaped_content = line_content.replace("'", "''")
        
        # Create the value tuple: (summary_timestamp, summare_line, summary_text)
        value_tuple = f"({sql_timestamp}, {line_num + 1}, '{escaped_content}')"
        values.append(value_tuple)
    
    if not values:
        return "-- No content generated for insertion."

    # Construct the final single BULK INSERT statement
    insert_statement = (
        "INSERT INTO defense.Situation_Awareness_Summary (summary_timestamp, summare_line, summary_text) VALUES \n"
        + ",\n".join(values) + ";"
    )
        
    return insert_statement

# --- Main Pipeline Execution ---

# Setup Connection and API Client
try:
    conn = cmldata.get_connection(CONNECTION_NAME)
    # The user provided the API_KEY as a string in the prompt; use it directly.
    client = OpenAI(
      base_url=BASE_URL,
      api_key=API_KEY
    )
except Exception as e:
    print(f"Failed to initialize connection or API client: {e}")
    sys.exit(1)

# 1. Data Retrieval
try:
    SQL_QUERY = "SELECT * FROM defense.lagebild WHERE harbour_name in (select name from defense.baltic_sea_harbours where country = 'Germany')"
    dataframe = conn.get_pandas_dataframe(SQL_QUERY)
    
    # --- NEW LIMITING LOGIC ---
    MAX_CHAR_LIMIT = 32000
    
    # Start with the full DataFrame
    df_to_use = dataframe
    
    # Convert the full DataFrame to Markdown to check its size
    df_content_string = df_to_use.to_markdown(index=False)
    
    # Check if the generated string exceeds the limit
    if len(df_content_string) > MAX_CHAR_LIMIT:
        print(f"Warning: Data size ({len(df_content_string)} chars) exceeds the {MAX_CHAR_LIMIT} limit. Truncating rows...")
        
        # Estimate the maximum number of rows based on the original length
        # We divide the total limit by the length of the string per row
        avg_row_length = len(df_content_string) / len(dataframe)
        max_rows = int(MAX_CHAR_LIMIT / avg_row_length) - 1 # Subtract 1 for header space

        # Truncate the DataFrame to the estimated max_rows
        df_to_use = dataframe.head(max_rows)
        
        # Re-generate the markdown string with the limited rows
        df_content_string = df_to_use.to_markdown(index=False)
        print(f"Truncated data to {len(df_to_use)} rows. New size: {len(df_content_string)} characters.")


except Exception as e:
    print(f"Failed to retrieve or process data: {e}")
    # Ensure connection is closed on failure
    if 'conn' in locals():
        conn.close()
    sys.exit(1)
# 2. Prompt Construction
prompt_template = """
--- MILITARY SITUATION REPORT ---
Analyst: military situation awreness - Please Analyse and provide a concise, pretty summary based on the data below.

Output Format:

Situation Awareness Summary (current data and time ) 

Number of all event analized is 30 recent observations, primarily driven by Buoy and AIS data sources. The analysis identifies Rostock as the primary current hotspot.

1. High-Priority Threat Assessment (Critical) - The single highest-priority event requires immediate investigation
2. Geographic Hotspots and AnomaliesThe most active regions are centered on German ports
3. Conclusion and Recommendation - example Highest Priority: Focus ASW resources immediately on the 0.89 km proximity zone near Rostock to classify the POSSIBLE_SUBMARINE object. Verification: Assign Maritime Patrol Aircraft (MPA) or Unmanned Surface Vessels (USVs) to verify the 203.6 and 157.9 magnetic anomalies reported near Kiel and Rostock.

AIS Sanctions: No vessels with known sanctions were detected in the vicinity of the assessed harbors during this reporting period.

Data:
{}

END OF DATA.
"""
prompt = prompt_template.format(df_content_string)

# 3. LLM Execution Setup
analysis_start_time = datetime.now()
timestamp_str = analysis_start_time.strftime("%Y%m%d_%H%M%S")
report_filename = f"llm_analysis_report_{timestamp_str}.txt"
sql_filename = f"llm_summary_insert_{timestamp_str}.sql"
llm_response_buffer = StringIO()


print(f"Starting analysis and writing report to {report_filename}...")

# 4. LLM Streaming and Capture
try:
    with open(report_filename, 'w', encoding='utf-8') as outfile:
        completion = client.chat.completions.create(
            model=MODEL_ID,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            top_p=0.7,
            max_tokens=2048,
            stream=True
        )
        
        for chunk in completion:
            content = chunk.choices[0].delta.content
            if content is not None:
                outfile.write(content)
                llm_response_buffer.write(content)
                print(".", end="", flush=True)

    llm_summary_text = llm_response_buffer.getvalue()

except Exception as e:
    print(f"\nError during LLM streaming: {e}")
    conn.close()
    sys.exit(1)

# 5. SQL Generation and Execution
try:
    # Generate the bulk INSERT statement
    sql_statement = generate_summary_sql(llm_summary_text, analysis_start_time) 
    
    with conn.get_base_connection() as base_conn:
        with base_conn.cursor() as cursor:
            # 3. Execute the DML statement (INSERT)
            cursor.execute(sql_statement)
            print("Bulk insertion into defense.Situation_Awareness_Summary complete.")    

except Exception as e:
    print(f"\nError during SQL DML Report Insert: {e}")
    
finally:
    # 6. Close the connection
    conn.close()
    print("\nDatabase connection closed.")
