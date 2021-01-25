import requests
import pandas as pd
import numpy as np

#note to self: to get US states shapefiles, use vega datasets
#states = alt.topo_feature(data.us_10m.url, 'states')


#Import SAHIE data
HOST = "https://api.census.gov/data"
dataset = "timeseries/healthins/sahie?"
base_url = "/".join([HOST, dataset])
numeric_cols = ['NUI_PT', 'PCTUI_PT', 'time']


##potentially unneeded code
get_vars_age = ['NAME', 'STABREV', 'NUI_PT', 'PCTUI_PT', 'AGECAT', 'AGE_DESC']

full_url_age = base_url + "get=" + ",".join(get_vars_age) + "&for=state:*&time=from+2008"
r_age = requests.get(full_url_age)

df_age = pd.DataFrame(data=r_age.json())

df_age.columns = df_age.iloc[0]
df_age = df_age[1:]
for col in numeric_cols:
    df_age[col] = pd.to_numeric(df_age[col])
##




get_vars_race = ['NAME', 'STABREV', 'NUI_PT', 'PCTUI_PT', 'RACECAT', 'RACE_DESC']

full_url_race = base_url + "get=" + ",".join(get_vars_race) + "&for=state:*&time=from+2008"
r_race = requests.get(full_url_race)

df_race = pd.DataFrame(data=r_race.json())

df_race.columns = df_race.iloc[0]
df_race = df_race[1:]
for col in numeric_cols:
    df_race[col] = pd.to_numeric(df_race[col])

df_race = df_race.rename({'NUI_PT': 'number_uninsured', 'PCTUI_PT': 'percent_uninsured',
                          'RACE_DESC': 'race_category'}, axis=1)
df_race['percent_uninsured'] = df_race['percent_uninsured'] / 100

##Subset to only white and black in 2018
df_race_viz = df_race[(df_race['race_category'] == 'White alone, not Hispanic') |
                      (df_race['race_category'] == 'Black alone, not Hispanic')]

df_race_viz['race_cat_short'] = df_race_viz['race_category'].str.slice(0, 5)
df_race_viz_2018 = df_race_viz[df_race_viz['time'] == 2018]


##Use the same dataset for an overall visualization
df_all_2008_2018 = df_race[df_race['race_category'] == 'All Races']

#Highlight New Mexico (largest decrease, at -12.4) and Massachusetts (smallest
#decrease, -1.1)
df_all_2008_2018['highlight'] = np.where(df_all_2008_2018['NAME'] == 'New Mexico', 1,
                               np.where(df_all_2008_2018['NAME'] == 'Massachusetts', 2, 0))




###KFF Data
file_path = 'Desktop\\Data Visualization\\Data files'

#Medicaid expansion and enrollment (as of June 2019)
expansion_filename = file_path + '\\medicaid_expansion_enrollment.csv'
expansion_df = pd.read_csv(expansion_filename, skiprows=2, header=0)
expansion_df = expansion_df[:52]

expansion_df.columns = ['Location', 'expanded_medicaid', 'total_medicaid_enrollment',
                        'expansion_enrollment', 'enrollment_newly_eligible',
                        'enrollment_not_newly_eligible']

expansion_df = expansion_df.fillna(0)
expansion_df_natl = expansion_df[expansion_df['Location'] =='United States']
expansion_df = expansion_df[expansion_df['Location'] !='United States']

#coverage by type
coverage_type = pd.DataFrame()

for i in range(2008, 2020):
    file_name = file_path + '\\coverage_by_state_' + str(i) + '.csv'
    df_year = pd.read_csv(file_name, skiprows=2, header=0)
    df_year = df_year[:52]
    df_year['year'] = i
    coverage_type = pd.concat([coverage_type, df_year])

coverage_type[coverage_type['Military']=='<.01'] = 0
coverage_type['Military'] = pd.to_numeric(coverage_type['Military'])
coverage_type['Other'] = coverage_type['Non-Group'] + coverage_type['Medicare'] + \
    coverage_type['Military']
coverage_type = coverage_type.drop(['Footnotes', 'Total', 'Non-Group', 
                                    'Medicare', 'Military'], axis=1)

coverage_type_natl = coverage_type[coverage_type['Location'] == 'United States']
coverage_type = coverage_type[coverage_type['Location'] != 'United States']

#add in expansion y/n and restrict to 2018 or 2008
coverage_type = coverage_type.merge(expansion_df[['Location', 'expanded_medicaid']],
                                    on='Location')
coverage_type_2018 = coverage_type[(coverage_type['year'] == 2018) | (coverage_type['year'] == 2008)]
coverage_type_2018 = coverage_type_2018.merge(df_all_2008_2018[['NAME', 'state']].drop_duplicates(),
                         left_on='Location', right_on='NAME')


#reshape national coverage
coverage_type_natl = pd.melt(coverage_type_natl, id_vars=['year'], 
                                  value_vars=['Employer', 'Medicaid', 'Uninsured', 
                                              'Other']).rename({'variable': 
                                              'source', 'value': 'percent'}, axis=1)
coverage_type_natl['color_order'] = np.where(coverage_type_natl['source'] == 'Medicaid', 1,
                                                  np.where(coverage_type_natl['source'] == 'Employer', 2,
                                                           np.where(coverage_type_natl['source'] == 'Other', 3, 4)))


###Uninsured rate by FPL
fpl_filename = file_path + '\\uninsured_by_fpl_2018.csv'
uninsured_fpl = pd.read_csv(fpl_filename, skiprows=2, header=0)
uninsured_fpl = uninsured_fpl[:52]
uninsured_fpl = uninsured_fpl.drop(['Total', 'Footnotes'], axis=1)

uninsured_fpl_natl = uninsured_fpl[uninsured_fpl['Location'] == 'United States']
uninsured_fpl = uninsured_fpl[uninsured_fpl['Location'] != 'United States']

uninsured_fpl = pd.melt(uninsured_fpl, id_vars=['Location'], 
                                value_vars=['Under 100%', '100-199%',
                                            '200-399%', '400%+']).rename({'variable': 
                                            'income_level', 'value': 'percent'}, axis=1)

uninsured_fpl = uninsured_fpl.merge(expansion_df[['Location', 'expanded_medicaid']],
                                    on='Location')

uninsured_fpl = uninsured_fpl.merge(df_all_2008_2018[['NAME', 'state']].drop_duplicates(), 
                                    left_on='Location', right_on='NAME').drop('NAME', axis=1)


#Medicaid coverage by age, both number and percent
def read_medicaid_age_data(file_extension):
    #read in csv file and remove extra rows at bottom
    df_filename = file_path + file_extension + '.csv'
    df = pd.read_csv(df_filename, skiprows=2, header=0)
    df = df[:52]

    #convert from wide to long
    df = df.set_index('Location')
    df.columns = df.columns.str.split('__', expand=True)
    df = df.stack(0).reset_index().rename(columns={'level_1':'Year'})

    return(df)


medicaid_age_perc = read_medicaid_age_data('//medicaid_age_percent')
medicaid_age_num = read_medicaid_age_data('//medicaid_age_number')

#separate state level and national data
medicaid_age_perc_natl = medicaid_age_perc[medicaid_age_perc['Location'] == 'United States']
medicaid_age_perc = medicaid_age_perc[medicaid_age_perc['Location'] != 'United States']

medicaid_age_perc_natl = pd.melt(medicaid_age_perc_natl, id_vars=['Year'], 
                                value_vars=['Adults 19-64', 'Children 0-18', 'Total']
                                ).rename({'variable': 'age_group', 'value': 'perc_medicaid'}, axis=1)



medicaid_age_num_natl = medicaid_age_num[medicaid_age_num['Location'] == 'United States']
medicaid_age_num = medicaid_age_num[medicaid_age_num['Location'] != 'United States']

medicaid_age_num_natl = pd.melt(medicaid_age_num_natl, id_vars=['Year'], 
                                value_vars=['Adults 19-64', 'Children 0-18', 'Total']
                                ).rename({'variable': 'age_group', 'value': 'num_medicaid'}, axis=1)
medicaid_age_num_natl = medicaid_age_num_natl[medicaid_age_num_natl['age_group'] != 'Total']
medicaid_age_num_natl['num_medicaid_mil'] = medicaid_age_num_natl['num_medicaid'] / 1000000


#Uninsured by age
uninsured_age = read_medicaid_age_data('//uninsured_by_age_2008_2019')
uninsured_age_natl = uninsured_age[uninsured_age['Location'] == 'United States']
uninsured_age = uninsured_age[uninsured_age['Location'] != 'United States']

uninsured_age_natl = pd.melt(uninsured_age_natl, id_vars=['Year'], 
                            value_vars=['Adults 19-64', 'Children 0-18', 'Total']
                            ).rename({'variable': 'age_group', 'value': 'perc_uninsured'}, axis=1)


#Medicaid income eligibility limits
elig_filename = file_path + '\\medicaid_parents_income_eligibility.csv'
elig_limits = pd.read_csv(elig_filename, skiprows=2, header=0)
elig_limits = elig_limits[:52]
elig_limits = elig_limits.drop(['Footnotes'], axis=1)

elig_limits = elig_limits.rename({'December 2009': 'January 2010'}, axis=1)
elig_limits_long = elig_limits.set_index('Location')
elig_limits_long = elig_limits_long.stack(0).reset_index()

elig_limits_long[['Month', 'Year']] = elig_limits_long['level_1'].str.split(' ', expand=True)
elig_limits_long = elig_limits_long.drop(['level_1', 'Month'], axis=1).rename(columns={0: 'elig_limit'})
elig_limits_long['Year'] = pd.to_numeric(elig_limits_long['Year'])

elig_limits_long = elig_limits_long[(elig_limits_long['Location'] != 'United States') & (elig_limits_long['Year'] >= 2008)]


####Datasets to write out:
#df_race_viz_2018
#df_all_2008_2018
#coverage_type_2018
#coverage_type_natl
#uninsured_fpl
#medicaid_age_perc_natl
#medicaid_age_num_natl
#uninsured_age_natl
#elig_limits_long

#write out datasets
df_race_viz_2018.to_csv(file_path + '\\df_race_viz_2018.csv')
df_all_2008_2018.to_csv(file_path + '\\df_all_2008_2018.csv')

coverage_type_2018.to_csv(file_path + '\\coverage_type_2018.csv')
coverage_type_natl.to_csv(file_path + '\\coverage_type_natl.csv')

uninsured_fpl.to_csv(file_path + '\\uninsured_fpl.csv')
medicaid_age_perc_natl.to_csv(file_path + '\\medicaid_age_perc_natl.csv')
medicaid_age_num_natl.to_csv(file_path + '\\medicaid_age_num_natl.csv')
uninsured_age_natl.to_csv(file_path + '\\uninsured_age_natl.csv')
elig_limits_long.to_csv(file_path + '\\elig_limits_long.csv')