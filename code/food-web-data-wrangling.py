import pandas as pd
import csv
from os import listdir

path = r'../Data/FoodsWeb_44622070'
filenames = [path + '/' + filename for filename in listdir(path) if '.csv' in filename]

df_food_web = pd.DataFrame()
 
for curr_file in filenames:
    file = open(curr_file, mode='r')
    food_web_src = (file.readline().split('",')[0])[1:]
    
    df_curr = pd.read_csv(curr_file, skiprows=1, encoding='latin-1')
    food_web_location = df_curr.columns[0]
    df_curr = df_curr.rename(columns={df_curr.columns[0]:'predator_species'})
    df_curr_melt = pd.melt(df_curr, id_vars=df_curr.columns[0], value_vars=df_curr.columns[1:], var_name='prey_species', value_name='feeds')
    df_curr_melt['predator_species'] = df_curr_melt.predator_species.str.upper()
    df_curr_melt['prey_species'] = df_curr_melt.prey_species.str.upper()
    df_curr_melt['feeds'] = df_curr_melt.feeds.fillna(0)
    df_curr_melt['feeds'] = df_curr_melt.feeds.astype('str')
    df_curr_melt['food_web_src'] = food_web_src.upper()
    df_curr_melt['food_web_location'] = food_web_location.upper()
    df_curr_melt['file_src'] = curr_file
    df_food_web = df_food_web.append(df_curr_melt)
        
df_vertices = pd.DataFrame()    
df_vertices['name'] = pd.concat([df_food_web['prey_species'], df_food_web['predator_species']]).drop_duplicates().reset_index(drop=True)
df_vertices['id'] = df_vertices.index

df_edges = df_food_web.merge(df_vertices.rename(columns={'id': 'src'}), left_on='predator_species', right_on='name')
df_edges = df_edges.merge(df_vertices.rename(columns={'id': 'dst'}), left_on='prey_species', right_on='name')
df_edges = df_edges[(df_edges.feeds!='0') & (df_edges.feeds!='0.0')]
df_edges = df_edges[['src','dst']]

df_edges.to_csv('../Data/Processed/food_web_edges.csv', encoding='utf-8', header=True, index=False, sep=';', quoting=csv.QUOTE_ALL)
df_vertices.to_csv('../Data/Processed/food_web_vertices.csv', encoding='utf-8', header=True, index=False, sep=';', quoting=csv.QUOTE_ALL)