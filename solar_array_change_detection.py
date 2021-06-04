import pandas as pd
from file_handling_docker import *
from functions import *

print("Function: Solar Array Change Detection")
print("Release: 0.0.1")
print("Date: 2021-05-27")
print("Author: Brian Neely")
print()
print()
print("A function to detect and categorize solar arrays from multiple electric meters.")
print("Requires Meter ID, Solar Irradiance, Datetime, and Production")
print()
print()

# Import Data
data = open_unknown_csv("data_in.csv", ',')

# Start Function
# Assigning Function Variables
data = data
prod_col = "Production (KW)"
datetime_col = "Datetime"
meter_col = "Meter_ID"
solar_col = "Solar Irradiance"

# Assign datetime_col to datetime
data[datetime_col] = pd.to_datetime(data[datetime_col])

# Get list of unique meters
meter_lst = dedupe_list(data[meter_col])

# Get max production for each meter
max_prod_df = data[[meter_col, prod_col]].groupby([meter_col]).max()

# Make new column of day
day_col = 'Day func'
data[day_col] = data[datetime_col].dt.date

# Make new column of Production per Irradiance
prd_per_slr_irrdnc_col = "Production per Solar Irradiance"
data[prd_per_slr_irrdnc_col] = data[prod_col] / data[solar_col]

# Make a list of DataFrames separated by meters
data_meter_df_lst = list()
for meter in meter_lst:
    data_meter_df_lst.append(data[data[meter_col] == meter])

# Make DataFrame to store data
meter_day_df = pd.DataFrame()

# For each of the meters, separate data into individual days
for meter_data in data_meter_df_lst:
    # meter_data = data_meter_df_lst[0]

    # Loop through days
    day_lst = dedupe_list(data[day_col])
    for day in day_lst:
        # Get the day records
        day_records = meter_data[meter_data[day_col] == day]

        # Meter ID
        meter = max(set(day_records[meter_col].tolist()), key=day_records[meter_col].tolist().count)

        # Get the production as a list
        prd = day_records[prod_col]

        # Sum of production in a day
        cum_prod = day_records[prod_col].sum()

        # Sum of solar irradiance in a day
        cum_slr_irrdnc = day_records[solar_col].sum()

        # Day Production per Irradiance
        day_prd_per_slr_irrdnc = cum_prod / cum_slr_irrdnc

        # Make new row
        day_data = {
            "Meter": meter,
            "Day": day,
            "Total Production": cum_prod,
            "Total Solar Irradiance": cum_slr_irrdnc,
            "Production per Irradiance": day_prd_per_slr_irrdnc
        }

        # Append row to DataFrame
        meter_day_df = meter_day_df.append(day_data, ignore_index=True)

# Write meter day to csv
meter_day_df.to_csv("test_out.csv", index=False)
