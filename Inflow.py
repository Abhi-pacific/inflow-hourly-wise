import pandas as pd
import streamlit as st


col1, col2, col3 = st.columns([0.1,4,1])
with col3:
    st.image('https://www.netimpactlimited.com/wp-content/uploads/2024/04/NetImpact-Logo-Final-Web-2.png')
with col2:
    st.subheader(f'Hourly wise Inflow, Abandon & Chat Count Day-1📈')

# Inflow file upload
raw_file_uploaded = st.file_uploader('Please upload the Inflow file here')
# Roaster file upload
roaster_file_uploaded = st.file_uploader('Please upload the Roaster file here')
if raw_file_uploaded and roaster_file_uploaded:
    st.success('Both file received successfully')
    st.write('Initiating the process....')
    
    
    #Converting the uploaded file in the excel engine

    raw_file = pd.ExcelFile(raw_file_uploaded)
    roaster_file = pd.ExcelFile(roaster_file_uploaded)

    # parsing the data for both the files.

    raw_parsed = raw_file.parse('Main File')
    roaster_raw = roaster_file.parse('Main Roaster')

    # droping the NA values for handling the # error in the excel.
    raw_parsed.dropna(subset=['Case First advisor assign time'],inplace=True)

    # Performing the feature engineering.
    raw_parsed['AHTold'] = raw_parsed['Case Closure Time'] - raw_parsed['Case First Response Time']
    raw_parsed['AHT new'] = raw_parsed['Case Closure Time'] - raw_parsed['Case Last advisor assign time']
    raw_parsed['ACT'] = raw_parsed['Case Last Response Time'] - raw_parsed['Case First Response Time']
    raw_parsed['hour to fetch'] = raw_parsed['Case First Response Time'].dt.hour
    raw_parsed['hour'] = raw_parsed['Case First Response Time'] - raw_parsed['Case Creation time']
    raw_parsed['Avg hold time'] = raw_parsed.groupby('hour to fetch')['hour'].transform('mean')
    raw_parsed['Avg assign time'] = raw_parsed['Case First advisor assign time'] - raw_parsed['Case Creation time']
    raw_parsed['Brand FRT'] = raw_parsed['Case First Response Time'] - raw_parsed['Case First advisor assign time']
    raw_parsed['Actual advisor'] = raw_parsed['Case First Assigned to Advisor'] == raw_parsed['Case Last Assigned to Advisor']

    # Removing unwanted columns from the roaster file
    roaster_raw = roaster_raw[['OLms ID','Emp Name','TL Name','Shift']]

    # Performing the merge operation
    raw_parsed = raw_parsed.merge(
    roaster_raw[['Emp Name','TL Name']],
    left_on='Case First Assigned to Advisor',
    right_on='Emp Name',
    how='left'
    )

    # Removing the Emp Name column
    raw_parsed.drop(columns=['Emp Name'],inplace=True)

    
    #Starting the Inflow dashboard preparation.
    
    # Inflow code

    # storing the fresh data in Inflow # error is not handled in this new data.
    Inflow_hourly = raw_file.parse('Main File')
    Inflow_hourly_count = Inflow_hourly.pivot_table(index=Inflow_hourly['Case Creation time'].dt.hour.values, values='Case Reference Id', aggfunc='count')
    
    # converting the 24 hour format in 12 hour format.
    Inflow_hourly_count.index = [pd.to_datetime(str(h), format='%H').strftime('%I %p') for h in Inflow_hourly_count.index]

    # fixing the index and renaming the column name.
    Inflow_hourly_count.reset_index(inplace=True)
    Inflow_hourly_count.columns = ['Time','Inflow']


    # Abondon Code
    Inflow_hourly_abondon = Inflow_hourly[((Inflow_hourly['Case Closed By Advisor'] == 'System') | (Inflow_hourly['Case Closed By Advisor'].empty))]
    Inflow_hourly_abondon_pivot  = Inflow_hourly_abondon.pivot_table(index=Inflow_hourly_abondon['Case Creation time'].dt.hour.values, values='Case Reference Id', aggfunc='count')
    Inflow_hourly_abondon_pivot.index = [pd.to_datetime(str(h),format='%H').strftime('%I %p') for h in Inflow_hourly_abondon_pivot.index]
    Inflow_hourly_abondon_pivot.reset_index(names='Time_1',inplace=True)
    Inflow_hourly_abondon_pivot.columns = ['Time_1','Abondon']

    
    
    #Merging the Inflow and abondon tables

    Inflow_hourly_count = Inflow_hourly_count.merge(
    Inflow_hourly_abondon_pivot,
    left_on='Time',
    right_on='Time_1',
    how='left'
    )

    # Droping the unwanted the column
    Inflow_hourly_count.drop(columns='Time_1',inplace=True)
    
    # Replacing the NaN values with the 0
    Inflow_hourly_count.fillna(0,inplace=True)

    Inflow_hourly_count['Abondon'] = Inflow_hourly_count['Abondon'].astype(int)
    
    # creating the Handle chats column.
    Inflow_hourly_count['Handle Chats'] = Inflow_hourly_count['Inflow'] - Inflow_hourly_count['Abondon']

    # wroking on the average ACT file.
    Average_of_ACT = raw_parsed.pivot_table(index=raw_parsed['Case Creation time'].dt.hour.values,values='ACT')
    Average_of_ACT.index = [pd.to_datetime(str(h),format='%H').strftime('%I %p') for h in Average_of_ACT.index]
    Average_of_ACT['ACT'] = Average_of_ACT['ACT'].dt.round('S')
    Average_of_ACT['ACT'] = Average_of_ACT['ACT'].astype(str).str.split('days').str[1]
    Average_of_ACT.reset_index(inplace=True)

    
    #Merging the Inflow and average ACT
    
    Inflow_hourly_count = Inflow_hourly_count.merge(
    Average_of_ACT,
    left_on='Time',
    right_on='index',
    how='left'
    )
    # droping the unwanted column
    Inflow_hourly_count.drop('index',axis=1,inplace=True)

    
    #Working on the average hold time.
    average_hold_time = raw_parsed.pivot_table(index=raw_parsed['Case Creation time'].dt.hour.values,values='Avg hold time',aggfunc='mean')

    # round off the micro-seconds
    average_hold_time['Avg hold time'] = average_hold_time['Avg hold time'].dt.round('S')
    
    average_hold_time['Avg hold time'] = average_hold_time['Avg hold time'].astype(str).str.split('days').str[1]
    average_hold_time.index = [pd.to_datetime(str(h),format='%H').strftime('%I %p') for h in average_hold_time.index]
    average_hold_time.reset_index(inplace=True)

    
    #Merging the Inflow and Avg hold time
    
    Inflow_hourly_count = Inflow_hourly_count.merge(
        average_hold_time,
        left_on='Time',
        right_on='index',
        how='left'
    )

    Inflow_hourly_count.drop('index',axis=1,inplace=True)


    # Working average assign time
    avg_assign_time = raw_parsed.pivot_table(index=raw_parsed['Case Creation time'].dt.hour.values,values='Avg assign time',aggfunc='mean')
    avg_assign_time['Avg assign time'] = avg_assign_time['Avg assign time'].dt.round('S').astype(str).str.split('days').str[1]
    avg_assign_time.index = [pd.to_datetime(str(h),format='%H').strftime('%I %p') for h in avg_assign_time.index]
    avg_assign_time.reset_index(inplace=True)

    
    #Merging the Avg assign time and Inflow
    
    Inflow_hourly_count = Inflow_hourly_count.merge(
    avg_assign_time,
    left_on='Time',
    right_on='index',
    how='left'
    )

    Inflow_hourly_count.drop('index',axis=1,inplace=True)
    
    
    # Working on the Brand FRT
    
    brand_FRT = raw_parsed.pivot_table(index=raw_parsed['Case Creation time'].dt.hour.values,values='Brand FRT',aggfunc='mean')
    brand_FRT['Brand FRT']= brand_FRT['Brand FRT'].dt.round('S').astype(str).str.split('days').str[1]
    brand_FRT.index = [pd.to_datetime(str(h),format='%H').strftime('%I %p') for h in brand_FRT.index]
    brand_FRT.reset_index(inplace=True)

    
    #Performing the merge operation on the Brand FRT and Inflow
    
    Inflow_hourly_count = Inflow_hourly_count.merge(
    brand_FRT,
    left_on='Time',
    right_on='index',
    how='left'
    )

    Inflow_hourly_count.drop('index',axis=1,inplace=True)

    # Showing the data frame

    st.dataframe(Inflow_hourly_count)

