"""
Streamlit Dashboard for NYC 311 Service Requests
Connects to AWS Athena for querying data stored in S3
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyathena import connect
import folium
from folium.plugins import HeatMap
from streamlit_folium import st_folium
import numpy as np

# Page config
st.set_page_config(
    page_title="NYC 311 Service Requests",
    page_icon="🏙️",
    layout="wide"
)

# AWS Athena connection
@st.cache_resource
def get_athena_connection():
    """Create Athena connection using Streamlit secrets"""
    conn = connect(
        s3_staging_dir=st.secrets["aws"]["s3_staging_dir"],
        region_name=st.secrets["aws"]["region"],
        aws_access_key_id=st.secrets["aws"]["access_key_id"],
        aws_secret_access_key=st.secrets["aws"]["secret_access_key"],
        schema_name='nyc_311'
    )
    return conn


@st.cache_data(ttl=3600)  # Cache for 1 hour
def run_query(sql):
    """Execute SQL query on Athena"""
    conn = get_athena_connection()
    df = pd.read_sql(sql, conn)
    return df


# Title and description
st.title("🏙️ NYC 311 Service Requests Dashboard")
st.markdown("""
Analyzing service requests across all five boroughs.
Data refreshed daily from [NYC Open Data](https://data.cityofnewyork.us/).
""")

# Sidebar filters
st.sidebar.header("Filters")

# Date range filter
date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(pd.Timestamp.now() - pd.Timedelta(days=365), pd.Timestamp.now()),
    key="date_range"
)

# Borough filter
boroughs = st.sidebar.multiselect(
    "Select Boroughs",
    options=['BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND'],
    default=['BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND']
)

# Status filter
status_filter = st.sidebar.multiselect(
    "Status",
    options=['Open', 'In Progress'],
    default=['Open', 'In Progress']
)

# Build WHERE clause for filters
where_clauses = []
if boroughs:
    borough_list = "', '".join(boroughs)
    where_clauses.append(f"borough IN ('{borough_list}')")
if status_filter:
    status_list = "', '".join(status_filter)
    where_clauses.append(f"status IN ('{status_list}')")
if len(date_range) == 2:
    start_date, end_date = date_range
    where_clauses.append(f"created_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'")

where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

# ===== METRICS ROW =====
st.header("📊 Key Metrics")

col1, col2 = st.columns(2)

# Total complaints
total_query = f"""
    SELECT COUNT(*) as total
    FROM service_requests_311
    WHERE {where_clause}
"""
total_complaints = run_query(total_query)['total'].iloc[0]
col1.metric("Total Complaints", f"{total_complaints:,}")

# Most common complaint
common_query = f"""
    SELECT complaint_type, COUNT(*) as count
    FROM service_requests_311
    WHERE {where_clause}
    GROUP BY complaint_type
    ORDER BY count DESC
    LIMIT 1
"""
common_complaint = run_query(common_query)
if not common_complaint.empty:
    col2.metric("Top Complaint Type", common_complaint['complaint_type'].iloc[0])
else:
    col2.metric("Top Complaint Type", "N/A")

# ===== Q1: TOP COMPLAINTS BY TYPE =====
st.header("📋 Top Complaint Types")

complaints_query = f"""
    SELECT 
        complaint_type,
        COUNT(*) as count
    FROM service_requests_311
    WHERE {where_clause}
    GROUP BY complaint_type
    ORDER BY count DESC
    LIMIT 15
"""
df_complaints = run_query(complaints_query)

fig_complaints = px.bar(
    df_complaints,
    x='count',
    y='complaint_type',
    orientation='h',
    title='Top 15 Complaint Types',
    labels={'count': 'Number of Complaints', 'complaint_type': 'Complaint Type'}
)
fig_complaints.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
st.plotly_chart(fig_complaints, use_container_width=True)

# ===== Q2: TOP AGENCIES =====
st.header("🏢 Agencies Receiving Most Complaints")

col1, col2 = st.columns(2)

with col1:
    agency_query = f"""
        SELECT 
            agency_name,
            COUNT(*) as count
        FROM service_requests_311
        WHERE {where_clause} AND agency_name IS NOT NULL
        GROUP BY agency_name
        ORDER BY count DESC
        LIMIT 10
    """
    df_agencies = run_query(agency_query)
    
    fig_agency = px.pie(
        df_agencies,
        values='count',
        names='agency_name',
        title='Top 10 Agencies by Complaint Volume'
    )
    st.plotly_chart(fig_agency, use_container_width=True)

with col2:
    # Show as table
    st.dataframe(
        df_agencies,
        column_config={
            "agency_name": "Agency",
            "count": st.column_config.NumberColumn("Complaints", format="%d")
        },
        hide_index=True,
        use_container_width=True
    )

# ===== Q3: COMPLAINTS BY BOROUGH =====
st.header("🗺️ Complaints by Borough")

borough_query = f"""
    SELECT 
        borough,
        COUNT(*) as count
    FROM service_requests_311
    WHERE {where_clause} AND borough IS NOT NULL
    GROUP BY borough
    ORDER BY count DESC
"""
df_borough = run_query(borough_query)

fig_borough = px.bar(
    df_borough,
    x='borough',
    y='count',
    title='Complaints by Borough',
    labels={'count': 'Number of Complaints', 'borough': 'Borough'}
)
st.plotly_chart(fig_borough, use_container_width=True)

# ===== Q4 & Q5: ZIP CODE ANALYSIS =====
st.header("📍 Zip Code Analysis")

col1, col2 = st.columns([1, 1])

with col1:
    # Top zip codes with borough info
    zip_query = f"""
        SELECT 
            incident_zip,
            borough,
            COUNT(*) as count
        FROM service_requests_311
        WHERE {where_clause} 
        AND incident_zip IS NOT NULL 
        AND borough IS NOT NULL
        GROUP BY incident_zip, borough
    """
    df_zip_borough = run_query(zip_query)
    
    # Convert zip codes to strings
    df_zip_borough['incident_zip'] = df_zip_borough['incident_zip'].astype(str)
    
    # Find most common borough for each zip code
    zip_to_borough = (
        df_zip_borough
        .groupby('incident_zip')
        .apply(lambda x: x.loc[x['count'].idxmax(), 'borough'])
        .reset_index()
        .rename(columns={0: 'primary_borough'})
    )
    
    # Get total counts per zip code
    df_zip = (
        df_zip_borough
        .groupby('incident_zip')['count']
        .sum()
        .reset_index()
        .sort_values('count', ascending=False)
        .head(15)
    )
    
    # Merge with borough info
    df_zip = df_zip.merge(zip_to_borough, on='incident_zip', how='left')
    
    # Create combined label: "10025 (MANHATTAN)"
    df_zip['zip_borough'] = df_zip['incident_zip'] + ' (' + df_zip['primary_borough'] + ')'
    
    # Create horizontal bar chart
    fig_zip = px.bar(
        df_zip,
        x='count',
        y='zip_borough',
        orientation='h',
        title='Top 15 Zip Codes',
        labels={'count': 'Number of Complaints', 'zip_borough': 'Zip Code (Borough)'}
    )
    
    # Force y-axis to be categorical and keep descending order
    fig_zip.update_layout(
        yaxis={
            'categoryorder': 'total ascending',
            'type': 'category'
        }
    )
    
    st.plotly_chart(fig_zip, use_container_width=True)

with col2:
    # Zip code search
    st.subheader("Search Specific Zip Code")
    selected_zip = st.text_input("Enter Zip Code", "10001")
    
    if selected_zip:
        zip_detail_query = f"""
            SELECT 
                complaint_type,
                COUNT(*) as count
            FROM service_requests_311
            WHERE {where_clause} 
            AND incident_zip = '{selected_zip}'
            GROUP BY complaint_type
            ORDER BY count DESC
            LIMIT 10
        """
        df_zip_detail = run_query(zip_detail_query)
        
        if not df_zip_detail.empty:
            st.write(f"**Top Complaints in {selected_zip}:**")
            st.dataframe(df_zip_detail, hide_index=True, use_container_width=True)
        else:
            st.info(f"No complaints found for zip code {selected_zip}")

# ===== Q6: REPEATED COMPLAINTS =====
st.header("🔁 Repeated Complaints from Same Address")

repeated_query = f"""
    SELECT 
        incident_address,
        complaint_type,
        COUNT(*) as complaint_count,
        COUNT(DISTINCT DATE(created_date)) as days_with_complaints
    FROM service_requests_311
    WHERE {where_clause} 
    AND incident_address IS NOT NULL
    GROUP BY incident_address, complaint_type
    HAVING COUNT(*) > 3
    ORDER BY complaint_count DESC
    LIMIT 20
"""
df_repeated = run_query(repeated_query)

if not df_repeated.empty:
    st.dataframe(
        df_repeated,
        column_config={
            "incident_address": "Address",
            "complaint_type": "Complaint Type",
            "complaint_count": st.column_config.NumberColumn("Total Complaints", format="%d"),
            "days_with_complaints": st.column_config.NumberColumn("Days with Complaints", format="%d")
        },
        hide_index=True,
        use_container_width=True
    )
else:
    st.info("No repeated complaints found with current filters")

# ===== Q7: HEATMAP =====
st.header("🔥 Complaint Heatmap")

# Limit to 10K points for performance
heatmap_query = f"""
    SELECT 
        latitude,
        longitude,
        borough
    FROM service_requests_311
    WHERE {where_clause}
    AND latitude IS NOT NULL 
    AND longitude IS NOT NULL
    LIMIT 10000
"""
df_geo = run_query(heatmap_query)

if not df_geo.empty:
    # Create folium map centered on NYC
    m = folium.Map(
        location=[40.7128, -74.0060],
        zoom_start=11,
        tiles='OpenStreetMap'
    )
    
    # Prepare heatmap data
    heat_data = [[row['latitude'], row['longitude']] for _, row in df_geo.iterrows()]
    
    # Add heatmap layer
    HeatMap(
        heat_data,
        radius=8,
        blur=10,
        max_zoom=13,
        gradient={0.4: 'blue', 0.65: 'lime', 0.8: 'yellow', 1.0: 'red'}
    ).add_to(m)
    
    # Display map
    st_folium(m, width=1400, height=600)
    
    st.caption(f"Showing {len(df_geo):,} complaints (limited to 10,000 for performance)")
else:
    st.info("No geo-located complaints found with current filters")

# ===== STATUS OVER TIME =====
st.header("📈 Complaints Over Time")

time_series_query = f"""
    SELECT 
        DATE(created_date) as date,
        status,
        COUNT(*) as count
    FROM service_requests_311
    WHERE {where_clause}
    GROUP BY DATE(created_date), status
    ORDER BY date
"""
df_time = run_query(time_series_query)

if not df_time.empty:
    df_time['date'] = pd.to_datetime(df_time['date'])
    
    fig_time = px.line(
        df_time,
        x='date',
        y='count',
        color='status',
        title='Complaints Over Time by Status',
        labels={'count': 'Number of Complaints', 'date': 'Date'}
    )
    st.plotly_chart(fig_time, use_container_width=True)

# Footer
st.markdown("---")
st.caption("Data source: [NYC Open Data - 311 Service Requests](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9)")