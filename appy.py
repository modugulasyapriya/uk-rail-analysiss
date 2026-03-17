import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.title("🚆 UK Rail Ticket Dashboard")

session = get_active_session()

# ---------------------------
# 1 MOST POPULAR ROUTES
# ---------------------------

query1 = """
SELECT 
Departure_Station,
Arrival_Destination,
COUNT(*) AS Ticket_Count
FROM train_tickets
GROUP BY Departure_Station, Arrival_Destination
ORDER BY Ticket_Count DESC
LIMIT 10
"""

df1 = session.sql(query1).to_pandas()

st.subheader("Top 10 Popular Routes")
st.bar_chart(df1.set_index("DEPARTURE_STATION")["TICKET_COUNT"])


# ---------------------------
# 2 PEAK TRAVEL HOURS
# ---------------------------

query2 = """
SELECT 
DATE_PART('HOUR', Departure_Time) AS Departure_Hour,
COUNT(*) AS Number_of_Journeys
FROM train_tickets
GROUP BY Departure_Hour
ORDER BY Departure_Hour
"""

df2 = session.sql(query2).to_pandas()

st.subheader("Peak Travel Hours")
st.line_chart(df2.set_index("DEPARTURE_HOUR")["NUMBER_OF_JOURNEYS"])


# ---------------------------
# 3 TICKET TYPE REVENUE
# ---------------------------

query3 = """
SELECT 
Ticket_Type,
SUM(Price) AS Total_Revenue
FROM train_tickets
GROUP BY Ticket_Type
"""

df3 = session.sql(query3).to_pandas()

st.subheader("Revenue by Ticket Type")
st.bar_chart(df3.set_index("TICKET_TYPE")["TOTAL_REVENUE"])


# ---------------------------
# 4 JOURNEY STATUS
# ---------------------------

query4 = """
SELECT 
Journey_Status,
COUNT(*) AS Total_Journeys
FROM train_tickets
GROUP BY Journey_Status
"""

df4 = session.sql(query4).to_pandas()

st.subheader("Journey Status Distribution")
st.bar_chart(df4.set_index("JOURNEY_STATUS")["TOTAL_JOURNEYS"])


# ---------------------------
# 5 REVENUE OVER TIME
# ---------------------------

query5 = """
SELECT 
Date_of_Journey,
SUM(Price) AS Revenue
FROM train_tickets
GROUP BY Date_of_Journey
ORDER BY Date_of_Journey
"""

df5 = session.sql(query5).to_pandas()

st.subheader("Revenue Over Time")
st.line_chart(df5.set_index("DATE_OF_JOURNEY")["REVENUE"])
