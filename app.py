import streamlit as st
import dask.dataframe as dd
from plotly import express as px
from PIL import Image
import datetime
import os

st.set_page_config(layout="wide")


root = os.getcwd()
# Read the data using Dask
df = dd.read_parquet(f"{root}\data\parquet\MERGED.parquet", engine='pyarrow')


#--------------------------------------#-----------------------------------------#-----------------------------#-------------------------------#----------------------------
# sample for quick analysis
# view = df[df["event_type"] == "view"].random_split([0.1, 0.9])[0].head(500)
# purchase = df[df["event_type"] == "purchase"].random_split([0.1, 0.9])[0].head()
# cart = df[df["event_type"] == "cart"].random_split([0.1, 0.9])[0].head(500)


# df = dd.concat([view, purchase, cart], axis=0)
#------------------------------------------#-----------------------#----------------------------#-------------------------------#----------------------------



# Extract date and time from 'event_time'
df["event_time"] = dd.to_datetime(df["event_time"], utc = True)
df["day"] = df["event_time"].dt.day
df["hour"] = df["event_time"].dt.hour





# Display the logo and title
col1, col2 = st.columns([0.2, 0.8])

image = Image.open(f"{root}\data\logo\logo.jpg")

with col1:
    st.image(image, use_column_width=True)
with col2:
    st.title("E-commerce Data Analysis")
    st.subheader("Sample Data from Olist")
    st.write("This application displays a sample of the e-commerce dataset and allows for further analysis.")





# Display the dataset KPIs
_,col3, col4 , col5 = st.columns([0.2, 0.8/3, 0.8/3, 0.8/3])


total_orders = df[df["event_type"] == "purchase"]["event_type"].count().compute()
total_revenue = df[df["event_type"] == "purchase"]["price"].sum().compute()
AOV = total_revenue / total_orders

with col3:
    st.subheader("Total Transactions")
    st.metric("Total Orders", f"{total_orders/1000:.2f}K")
with col4:  
    st.subheader("Revenue Metrics")
    st.metric("Total Revenue", f"${total_revenue/1_000_000:.2f}M")
with col5:
    st.subheader("Average Order Value")
    st.metric("Average Order Value (AOV)", f"${AOV:.2f}")





# highest_revenue by brand
_, col6 = st.columns([0.2, 0.8])
highest_revenue_brand = df[df["event_type"] == "purchase"].groupby("brand")["price"].sum().nlargest(5).reset_index().compute()

fig = px.bar(highest_revenue_brand, x="brand", y="price", title="Top 5 Brands by Revenue")

with col6:
    st.subheader("Top 5 Brands by Revenue")
    st.plotly_chart(fig, use_container_width=True)





# Most viewed products
_, col7 = st.columns([0.2, 0.8])
most_viewed_products = df[df["event_type"] == "view"].groupby("category_code")["event_type"].count().nlargest(5).reset_index().compute()

with col7:
    st.subheader("Top 5 Most Viewed Products")
    fig2 = px.bar(most_viewed_products, x="category_code", y="event_type", title="Top 5 Most Viewed Products")
    st.plotly_chart(fig2, use_container_width=True)



# Sales trend over time
_, col8 = st.columns([0.2, 0.8])

sales_trend = df[df["event_type"] == "purchase"].groupby("day")["price"].sum().reset_index().compute()
sales_trend.columns = ['day', 'price']

with col8:
    st.subheader("📈 Sales Trend Over Time")
    fig3 = px.line(
        sales_trend,
        x="day",
        y="price",
        title="Daily Sales Trend",
        labels={"price": "Revenue", "day": "day"}
    )
    st.plotly_chart(fig3, use_container_width=True)



# visitors by hour
_, col9 = st.columns([0.2, 0.8])
visitors_by_hour = df[df["event_type"] == "view"].groupby("hour")["event_type"].count().reset_index().compute()
visitors_by_hour.columns = ['hour', 'visitors']

with col9:
    st.subheader("👥 Visitors by Hour")
    fig4 = px.line(
        visitors_by_hour,
        x="hour",
        y="visitors",
        title="Visitors by Hour of the Day",
        labels={"visitors": "Number of Visitors", "hour": "Hour"}
    )
    st.plotly_chart(fig4, use_container_width=True)

# Revenue by weekday
_, col10 = st.columns([0.2, 0.8])

df['weekday'] = df['event_time'].dt.weekday
revenue_by_weekday = df[df["event_type"] == "purchase"].groupby("weekday")["price"].sum().reset_index().compute()

with col10:
    st.subheader("📅 Revenue by Weekday")
    fig5 = px.bar(
        revenue_by_weekday,
        x="weekday",
        y="price",
        title="Revenue by Weekday",
        labels={"price": "Revenue", "weekday": "Weekday"}
    )
    st.plotly_chart(fig5, use_container_width=True)