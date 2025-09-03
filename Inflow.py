import pandas as pd
import streamlit as st


col1, col2, col3 = st.columns([0.1,4,1])
with col3:
    st.image('https://www.netimpactlimited.com/wp-content/uploads/2024/04/NetImpact-Logo-Final-Web-2.png')
with col2:
    st.subheader(f'Hourly wise Inflow, Abandon & Chat Count Day-1📈')

c1 = st.container()
c2 = st.container()
c3 = st.container()

with c1:
    # Inflow file upload
    raw_file_uploaded = st.file_uploader('Please upload the Inflow file here')
    # Roaster file upload
    roaster_file_uploaded = st.file_uploader('Please upload the Roaster file here')
if raw_file_uploaded and roaster_file_uploaded:
    with c1:
        st.success('Both file received successfully')
    
    
    #Converting the uploaded file in the excel engine

    raw_file = pd.ExcelFile(raw_file_uploaded)
    roaster_file = pd.ExcelFile(roaster_file_uploaded)

    # parsing the data for both the files.

    raw_parsed = raw_file.parse('Main File')
    roaster_raw = roaster_file.parse('Roaster')

    # converting to the lower case
    roaster_raw['Emp Name']= roaster_raw['Emp Name'].str.lower()
    raw_parsed['Case First Assigned to Advisor'] = raw_parsed['Case First Assigned to Advisor'].str.lower()

    # storeing the length of the data for MIS dash board
    overall_inflow = len(raw_parsed)

    # droping the NA values for handling the # error in the excel.
    raw_parsed.dropna(subset=['Case First advisor assign time'],inplace=True)

    # total inflow variable created for the mis add details
    total_inflow = len(raw_parsed)

    # Performing the feature engineering.
    raw_parsed['AHTold'] = raw_parsed['Case Closure Time'] - raw_parsed['Case First Response Time']
    raw_parsed['AHT new'] = raw_parsed['Case Closure Time'] - raw_parsed['Case Last advisor assign time']
    raw_parsed['ACT'] = raw_parsed['Case Last Response Time'] - raw_parsed['Case First Response Time']
    raw_parsed['hour to fetch'] = raw_parsed['Case First Response Time'].dt.hour
    raw_parsed['Response Delay'] = raw_parsed['Case First Response Time'] - raw_parsed['Case Creation time']
    raw_parsed['Avg hold time'] = raw_parsed.groupby('hour to fetch')['Response Delay'].transform('mean')
    raw_parsed['Avg assign time'] = raw_parsed['Case First advisor assign time'] - raw_parsed['Case Creation time']
    raw_parsed['Brand FRT'] = raw_parsed['Case First Response Time'] - raw_parsed['Case First advisor assign time']
    raw_parsed['Actual advisor'] = raw_parsed['Case First Assigned to Advisor'] == raw_parsed['Case Last Assigned to Advisor']

    # Average BRAND FRT (Overall, Including Outliers)
    avg_brand_frt = raw_parsed['Brand FRT'].mean().round('1s')
    avg_brand_frt_count = len(raw_parsed)
    avg_brand_frt = str(avg_brand_frt).split('days')[1].strip()

    # Calculation for average TAT (ACT)
    average_tat = raw_parsed['ACT'].mean()
    average_tat = average_tat.round('1s')
    average_tat = str(average_tat).split('days')[1].strip()

    # Case TAT unique cases (Cases created Day-1)
    tat_hours_smaller_4 = raw_parsed[(raw_parsed['ACT'].dt.components['hours'] < 4)]
    tat_hours_sm_4 = tat_hours_smaller_4['ACT'].mean().round('1s')
    tat_hours_sm_4 = str(tat_hours_sm_4).split('days')[1].strip()

    # Serviceable from Unique Inflow

    unique_inflow = raw_parsed[~(raw_parsed['Closed Reason'] == 'UNRESPONSIVE')]
    len_unique_inflow = len(unique_inflow)

    unique_inflow_rev = raw_parsed[(raw_parsed['Closed Reason'] == 'UNRESPONSIVE')]
    len_unique_inflow_rev = len(unique_inflow_rev)


    # created temp  data for BRAND FRT (Time) -SL Breached
    new_raw_parsed = raw_parsed
    new_raw_parsed = new_raw_parsed[(new_raw_parsed['Brand FRT'].dt.components.minutes < 1)]
    new_raw_parsed_rev = raw_parsed[(raw_parsed['Brand FRT'].dt.components.minutes >= 1)]
    new_raw_parsed_rev_len = new_raw_parsed_rev.__len__()
    avg_brand_frt_sl = new_raw_parsed_rev['Brand FRT'].mean().round('1s')
    avg_brand_frt_sl = str(avg_brand_frt_sl).split('days')[1].strip()

    avg_brand_frt_outlier = new_raw_parsed['Brand FRT'].mean().round('1s')
    avg_brand_frt_outlier_count = len(new_raw_parsed)
    avg_brand_frt_outlier = str(avg_brand_frt_outlier)
    avg_brand_frt_outlier = avg_brand_frt_outlier.split('days')[1].strip()

    # Removing unwanted columns from the roaster file
    roaster_raw = roaster_raw[['OLms ID','Emp Name','TL Name','Shift','present_attendance']]

    # count of present employees
    present_emp = (roaster_raw['present_attendance'] == 'P').sum()

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
    Inflow_hourly_abondon = Inflow_hourly[((Inflow_hourly['Case Closed By Advisor'] == 'System') |(Inflow_hourly['Case Closed By Advisor'].isnull()))]
    Inflow_hourly_abondon_pivot  = Inflow_hourly_abondon.pivot_table(index=Inflow_hourly_abondon['Case Creation time'].dt.hour.values, values='Case Reference Id', aggfunc='count')
    Inflow_hourly_abondon_pivot.index = [pd.to_datetime(str(h),format='%H').strftime('%I %p') for h in Inflow_hourly_abondon_pivot.index]
    Inflow_hourly_abondon_pivot.reset_index(names='Time_1',inplace=True)
    Inflow_hourly_abondon_pivot.columns = ['Time_1','Abondon']

    # abandon_count 
    abandon_count = Inflow_hourly_abondon_pivot['Abondon'].sum()
    
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
    average_hold_time['Avg hold time'] = average_hold_time['Avg hold time'].dt.round('s')
    
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


    # Staring the MIS add details
    with c2:
        chat = pd.Series(['MTD Inflow Data till ','MTD Inflow Data till ','MTD Inflow Data till ',
                    'Inflow Data','Inflow Data','Inflow Data','Inflow Data','Inflow Data',
                    'Productivity','Productivity','Productivity','Productivity','Productivity','Productivity','Productivity',
                    'Tagging',
                    'Brand FRT','Brand FRT','Brand FRT','Brand FRT','Brand FRT',
                    'Case TAT','Case TAT','Case TAT','Case TAT',
                    'Repeat'
                    ])
        
        Particular = pd.Series(['Total Overall Inflow (Including Serviceable Opportunities)','Total Inflow Handled','Total Abandon','Overall Inflow (Including Serviceable Opportunities)','Total Inflow','Serviceable from Unique Inflow','NRR from Unique Inflow (Unresponsive)','Abandon','Advisors Logged IN (X-L1)',
                            'Productivity as per the Approved FTE (22 with 34% Shrinkage)','ADVISOR PRODUCTIVITY OVERALL - Overall Cases','ADVISOR PRODUCTIVITY OVERALL- Serviceable Cases',
                            'Productivity as per the Actual HC with Actual Shrinkage','ACTUAL ADVISOR PRODUCTIVITY OVERALL - Overall Cases','ACTUAL ADVISOR PRODUCTIVITY OVERALL- Serviceable Cases',
                            'Tagged Count on Service Portal (On Closed cases)','BRAND FRT (Overall, Including Outliers)','Brand FRT Unique Cases (Outliers Removed)',
                            'BRAND FRT (Achievement)','BRAND FRT (Time) -SL Breached','BRAND FRT SL Breached case count','Case TAT  (Overall, Including Outliers)',
                            'Case TAT unique cases (Cases created Day-1)','Case TAT (Achievement)','Case TAT  (Time)- SL Breached','Repeat (Customer Message Vs Case ID)']) 
        
        # created the dataframe 
        mis_add = pd.DataFrame({ 'chat': chat, 'Particular': Particular })

        # Asking user for the date to create new column in the dataframe
        date = st.text_input(f'Enter the Date')
        
        #inserting the values in the dataframe
        
        mis_add.loc[
        (mis_add['chat'] == 'Productivity') &
        (mis_add['Particular'] == 'Productivity as per the Approved FTE (22 with 34% Shrinkage)'),
        'Target'
        ] = 15

        mis_add.loc[
        (mis_add['chat'] == 'Brand FRT') & (mis_add['Particular'] == 'Brand FRT Unique Cases (Outliers Removed)'),'Target'
        ] = '< 1 Minutes'

        mis_add.loc[
        (mis_add['chat'] == 'Inflow Data') & (mis_add['Particular'] == 'Overall Inflow (Including Serviceable Opportunities)'),date
        ] = overall_inflow

        mis_add.loc[
        (mis_add['chat'] == 'Inflow Data') & (mis_add['Particular'] == 'Total Inflow'),date
        ] = total_inflow

        mis_add.loc[
        (mis_add['chat'] == 'Inflow Data') & (mis_add['Particular'] == 'Serviceable from Unique Inflow'),date
        ] = len_unique_inflow

        mis_add.loc[
        (mis_add['chat'] == 'Inflow Data') & (mis_add['Particular'] == 'NRR from Unique Inflow (Unresponsive)'),date
        ] = len_unique_inflow_rev

        mis_add.loc[
        (mis_add['chat'] == 'Inflow Data') & (mis_add['Particular'] == 'Abandon'),date
        ] = abandon_count

        mis_add.loc[
        (mis_add['chat'] == 'Productivity') & (mis_add['Particular'] == 'Productivity as per the Approved FTE (22 with 34% Shrinkage)'),'Target'
        ] = 15

        mis_add.loc[
        (mis_add['chat'] == 'Productivity') & (mis_add['Particular'] == 'Productivity as per the Approved FTE (22 with 34% Shrinkage)'),date
        ] = (total_inflow/15).__round__(0)

        mis_add.loc[
        (mis_add['chat'] == 'Productivity') & (mis_add['Particular'] == 'ADVISOR PRODUCTIVITY OVERALL - Overall Cases'),date
        ] = (total_inflow/15).__round__(0)

        mis_add.loc[
        (mis_add['chat'] == 'Productivity') & (mis_add['Particular'] == 'ADVISOR PRODUCTIVITY OVERALL- Serviceable Cases'),date
        ] = (total_inflow/15).__round__(0)

        mis_add.loc[
        (mis_add['chat'] == 'Productivity') & (mis_add['Particular'] =='Advisors Logged IN (X-L1)'), date
        ] = present_emp

        mis_add.loc[
        (mis_add['chat'] == 'Productivity') & (mis_add['Particular'] == 'Productivity as per the Actual HC with Actual Shrinkage'),date
        ] = (total_inflow/present_emp).__round__()

        mis_add.loc[
        (mis_add['chat'] == 'Productivity') & (mis_add['Particular'] == 'ACTUAL ADVISOR PRODUCTIVITY OVERALL - Overall Cases'),date
        ] = (total_inflow/present_emp).__round__()

        mis_add.loc[
        (mis_add['chat'] == 'Productivity') & (mis_add['Particular'] == 'ACTUAL ADVISOR PRODUCTIVITY OVERALL- Serviceable Cases'),date
        ] = (total_inflow/present_emp).__round__()

        mis_add.loc[
        (mis_add['chat'] == 'Tagging') & (mis_add['Particular'] == 'Tagged Count on Service Portal (On Closed cases)'),date
        ] = '100%'

        mis_add.loc[ 
        (mis_add['chat'] == 'Brand FRT') & (mis_add['Particular'] == 'BRAND FRT (Overall, Including Outliers)'),date
        ] = avg_brand_frt

        # "99.00%"
        s = (avg_brand_frt_outlier_count/avg_brand_frt_count)
        val_percent = f"{s * 100:.2f}%" 

        mis_add.loc[ 
        (mis_add['chat'] == 'Brand FRT') & (mis_add['Particular'] == 'Brand FRT Unique Cases (Outliers Removed)'),date
        ] = val_percent

        mis_add.loc[ 
        (mis_add['chat'] == 'Brand FRT') & (mis_add['Particular'] == 'Brand FRT Unique Cases (Outliers Removed)'),'Target'
        ] = '>95%'

        mis_add.loc[ 
        (mis_add['chat'] == 'Case TAT') & (mis_add['Particular'] == 'Case TAT unique cases (Cases created Day-1)'),'Target'
        ] = '< 4 hours'

        mis_add.loc[ 
        (mis_add['chat'] == 'Case TAT') & (mis_add['Particular'] == 'Case TAT (Achievement)'),'Target'
        ] = '> 90%'

        mis_add.loc[ 
        (mis_add['chat'] == 'Case TAT') & (mis_add['Particular'] == 'Case TAT (Achievement)'),date
        ] = '100%'

        mis_add.loc[ 
        (mis_add['chat'] == 'Brand FRT') & (mis_add['Particular'] == 'BRAND FRT (Time) -SL Breached'),date
        ] = avg_brand_frt_sl

        mis_add.loc[ 
        (mis_add['chat'] == 'Brand FRT') & (mis_add['Particular'] == 'BRAND FRT SL Breached case count'),date
        ] = new_raw_parsed_rev_len

        mis_add.loc[
        (mis_add['chat'] == 'Case TAT') & (mis_add['Particular'] == 'Case TAT  (Overall, Including Outliers)'),date
        ] = average_tat

        mis_add.loc[
        (mis_add['chat'] == 'Case TAT') & (mis_add['Particular'] == 'Case TAT unique cases (Cases created Day-1)'),date
        ] = tat_hours_sm_4

        mis_add.loc[
        (mis_add['chat'] == 'Repeat') & (mis_add['Particular'] == 'Repeat (Customer Message Vs Case ID)'),'Target'
        ] = 'Ratio-"1:4"'


        # code for the Leads dashboard

        #creating the pivot table for Leads
        leads_pivot = raw_parsed.pivot_table(
            index='TL Name',
            values='ACT',
            aggfunc='mean',
            margins=True,
            margins_name='Grand Total'
        )
        leads_pivot.reset_index(inplace=True)

        # fixing the format of the time by using the round '00:00:00'
        leads_pivot['ACT'] = (leads_pivot['ACT'].apply(lambda X : X.round('s'))).astype(str).str.split('days').str[1]


        # Creating the pivot table for the Brand FRT
        brand_frt_pivot = raw_parsed.pivot_table(
            index='TL Name',
            values='Brand FRT',
            aggfunc='mean',
            margins=True,
            margins_name='Grand Total'
        )

        brand_frt_pivot.reset_index(inplace=True)

        brand_frt_pivot['Brand FRT'] = (brand_frt_pivot['Brand FRT'].apply(lambda X : X.round('s'))).astype(str).str.split('days').str[1]

        # Renaming the column name, so that we can merge the pivot later 
        brand_frt_pivot.rename(columns={'TL Name':'TL Name 2'},inplace=True)

        # Creating the pivot table for the handle chat
        handle_chat_pivot = raw_parsed.pivot_table(
            index='TL Name',
            values='Case Reference Id',
            aggfunc='count',
            margins=True,
            margins_name='Grand Total'
        )

        handle_chat_pivot.reset_index(inplace=True)

        # renaming the column name
        handle_chat_pivot.rename(columns={'TL Name':'TL Name 3'},inplace=True)

        # Merging two pivot tables 
        leads_pivot = leads_pivot.merge(
            brand_frt_pivot[['TL Name 2','Brand FRT']],
            right_on='TL Name 2',
            left_on='TL Name',
            how='left'
        )
        leads_pivot.drop(columns=['TL Name 2'],inplace=True)

        # Merging the pivot table
        leads_pivot = leads_pivot.merge(
            handle_chat_pivot[['TL Name 3','Case Reference Id']],
            left_on='TL Name',
            right_on='TL Name 3',
            how='left'
        )

        leads_pivot.drop(columns=['TL Name 3'],inplace=True)

        # leads_pivot['ACT'] = pd.to_timedelta(leads_pivot['ACT'])
        # leads_pivot['Brand FRT'] = pd.to_timedelta(leads_pivot['Brand FRT'])


        # Working on the top 4 Brand FRT
        brand_frt_top4 = raw_parsed.sort_values('Brand FRT',ascending=False).head(4)

        # Removing the unwanted columns from the data frame
        brand_frt_top4 = brand_frt_top4[['Case Reference Id','Customer Phone','Case Creation time','Case First advisor assign time','Case First Assigned to Advisor','Case Last Assigned to Advisor','Case First Response Time','Notes','Brand FRT','TL Name']]
        # reseting the Index
        brand_frt_top4.reset_index(inplace=True)

        # Droping the index column
        brand_frt_top4.drop(columns='index',inplace=True)

        # fixing the format of the column
        brand_frt_top4['Brand FRT'] = brand_frt_top4['Brand FRT'].astype(str).str.split('days').str[1]


        # Working  on the top 4 AHT
        AHT_top4 = raw_parsed.sort_values('Response Delay',ascending=False).head(4)
        AHT_top4 = AHT_top4[['Case Reference Id','Customer Phone','Case Creation time','Case First advisor assign time','Case First Assigned to Advisor','Case Last Assigned to Advisor','Case First Response Time','Notes','Response Delay','TL Name']]
        AHT_top4['Response Delay'] = AHT_top4['Response Delay'].astype(str).str.split('days').str[1]
        AHT_top4.reset_index(inplace=True)
        AHT_top4.drop(columns='index',inplace=True)

        


    # Showing the data frame
    with c2:
        st.dataframe(Inflow_hourly_count)
    with c3:
        st.dataframe(mis_add)
        st.dataframe(leads_pivot)
        st.dataframe(brand_frt_top4)
        st.dataframe(AHT_top4)

    st.write('Thanks for using, have greate day 😊')

