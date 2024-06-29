#importing required libraries
import warnings
warnings.simplefilter('ignore')
import pandas as pd
import numpy as np
import json
import os
from os import path


MATCH_CSV_PATH = os.path.abspath(os.getcwd()) + '//data//match_data.csv'
DELIVERIES_CSV_PATH = os.path.abspath(os.getcwd()) + '//data//deliveries_data.csv'
PROCESSED_JS_FILES_PATH = os.path.abspath(os.getcwd()) + '//static//chart_js//'

# loading data 
matchdf = pd.read_csv(MATCH_CSV_PATH)
deliverydf = pd.read_csv(DELIVERIES_CSV_PATH)
runs = pd.DataFrame()


def data_cleaning():

    global matchdf, deliverydf, runs

    # change data type of column
    matchdf.match_id = matchdf.match_id.astype(int)
    deliverydf.wicket = deliverydf.wicket.astype(int)
    deliverydf.match_id = deliverydf.match_id.astype(int)

    # replace values
    matchdf = matchdf.replace({'Rising Pune Supergiant':'Rising Pune Supergiants','Delhi Daredevils':'Delhi Capitals'})
    deliverydf = deliverydf.replace({'Rising Pune Supergiant':'Rising Pune Supergiants','Delhi Daredevils':'Delhi Capitals'})

    # ADD new column match type 
    matchdf["type"] = "pre-qualifier"
    for year in range(2008, 2020):
        final_match_index = matchdf[matchdf['season']==year][-1:].index.values[0]
        matchdf.at[final_match_index, "type"] = "final"
        matchdf.at[final_match_index-1, "type"] = "qualifier-2"
        matchdf.at[final_match_index-2, "type"] = "eliminator"
        matchdf.at[final_match_index-3, "type"] =  "qualifier-1"

    #merge season value from the matches dataframe to deliveries
    runs = matchdf[['match_id','season']].merge(deliverydf, left_on = 'match_id', right_on = 'match_id', how = 'left')
        


def batsman_table_data_collection():
    
    global runs
    df = runs

    # total runs scored by batsman
    total_runs_scored_by_batsman = df
    total_runs_scored_by_batsman = total_runs_scored_by_batsman.groupby(['batsman'])['batsman_runs'].sum().reset_index()
    total_runs_scored_by_batsman.sort_values(ascending=False,by='batsman_runs',inplace=True) 
    total_runs_scored_by_batsman.rename(columns={'batsman_runs':'total_runs'},inplace=True)
    
    # total number of time 100s by batsman
    total_100s_by_batsman = df
    total_100s_by_batsman = total_100s_by_batsman.groupby(['match_id','batsman'])['batsman_runs'].sum().reset_index()
    total_100s_by_batsman = total_100s_by_batsman.rename(columns={'batsman_runs':'number_of_100s'})
    total_100s_by_batsman['number_of_100s'] = (total_100s_by_batsman.number_of_100s>=100)
    total_100s_by_batsman['number_of_100s'] = total_100s_by_batsman['number_of_100s'].astype(int)
    total_100s_by_batsman = total_100s_by_batsman.groupby(['batsman'])['number_of_100s'].sum().reset_index()
    total_100s_by_batsman.sort_values(ascending=False,by='number_of_100s',inplace=True) 
    
    # total number of time 50s by batsman
    total_50s_by_batsman = df
    total_50s_by_batsman = total_50s_by_batsman.groupby(['match_id','batsman'])['batsman_runs'].sum().reset_index()
    total_50s_by_batsman = total_50s_by_batsman.rename(columns={'batsman_runs':'number_of_50s'})
    total_50s_by_batsman['number_of_50s'] = ((total_50s_by_batsman.number_of_50s>=50)&(total_50s_by_batsman.number_of_50s<100))
    total_50s_by_batsman['number_of_50s'] = total_50s_by_batsman['number_of_50s'].astype(int)
    total_50s_by_batsman = total_50s_by_batsman.groupby(['batsman'])['number_of_50s'].sum().reset_index()
    total_50s_by_batsman.sort_values(ascending=False,by='number_of_50s',inplace=True) 
    
    # number of 6s by player
    number_of_6s_by_player = df[df.batsman_runs==6].groupby(['batsman'])['batsman_runs'].count().reset_index()
    number_of_6s_by_player.rename(columns = {'batsman_runs':'number_of_6s'},inplace=True)
    number_of_6s_by_player.sort_values(ascending=False,by='number_of_6s',inplace=True) 
    
    # number of 4s by player
    number_of_4s_by_player = df[df.batsman_runs==4].groupby(['batsman'])['batsman_runs'].count().reset_index()
    number_of_4s_by_player.rename(columns = {'batsman_runs':'number_of_4s'},inplace=True)
    number_of_4s_by_player.sort_values(ascending=False,by='number_of_4s',inplace=True) 
    
    # Highest score by batsman
    highest_score_by_batsman = df.groupby(['match_id','batsman'])['batsman_runs'].sum().reset_index()
    highest_score_by_batsman.rename(columns={'batsman_runs':'highest_score'},inplace=True)
    highest_score_by_batsman = highest_score_by_batsman.groupby(['batsman'])['highest_score'].max().reset_index()
    highest_score_by_batsman.sort_values(ascending=False,by='highest_score',inplace=True) 
    
    # merge all data in one dataframe
    batsman_table_data = total_runs_scored_by_batsman
    batsman_table_data = batsman_table_data.merge(highest_score_by_batsman, left_on = 'batsman', right_on = 'batsman', how = 'left')
    batsman_table_data = batsman_table_data.merge(total_100s_by_batsman, left_on = 'batsman', right_on = 'batsman', how = 'left')
    batsman_table_data = batsman_table_data.merge(total_50s_by_batsman, left_on = 'batsman', right_on = 'batsman', how = 'left')
    batsman_table_data = batsman_table_data.merge(number_of_6s_by_player, left_on = 'batsman', right_on = 'batsman', how = 'left')
    batsman_table_data = batsman_table_data.merge(number_of_4s_by_player, left_on = 'batsman', right_on = 'batsman', how = 'left')
    batsman_table_data = batsman_table_data.fillna(0)
    batsman_table_data.number_of_6s = batsman_table_data.number_of_6s.astype(int)
    batsman_table_data.number_of_4s = batsman_table_data.number_of_4s.astype(int)
    
    # dataframe convert to json formate
    df1 = batsman_table_data

    record = []
    for index,row in df1.iterrows():
        rec = []
        rec.append(row[0])
        rec.append(row[1])
        rec.append(row[2])
        rec.append(row[3])
        rec.append(row[4])
        rec.append(row[5])
        rec.append(row[6])
        record.append(rec)

    s = "var batsman_table_data = "+str(record)
    
    return s.strip()


# orange cap table data collection
def orangecap_table_data_collection():

    global runs
    df = runs
    # season wise orange cap
    per_season_runs = df.groupby(['season','batsman'])['batsman_runs'].sum().reset_index()
    per_season_runs.sort_values(ascending=False,by='batsman_runs',inplace=True) 
    orange_cap_season_wise = per_season_runs.groupby('season').head(1)
    
    # season wise number of innings played by batsman
    df1 = df
    df1['innings'] = 1
    df2 = df1.groupby(['match_id','season','batsman']).count().reset_index()
    matches_played = df2.groupby(['season','batsman']).count().reset_index()
    matches_played = matches_played[['season','batsman','innings']]
    # matches_played=matches_played.groupby(['batsman'])['innings'].sum().reset_index()
    matches_played.sort_values(ascending=False,by='innings',inplace=True) 

    # season wise highest score of batsman
    highest_score_per_season=df.groupby(['season','match_id','batsman'])['batsman_runs'].sum().reset_index()
    highest_score_per_season = highest_score_per_season.groupby(['season','batsman']).agg({'batsman_runs': {lambda x: x.nlargest(1)}}).reset_index()
    highest_score_per_season.rename(columns={'batsman_runs':'highest_score','<lambda>':''},inplace=True)
    
    # number of 100s season wise
    total_100s_by_batsman = df
    total_100s_by_batsman = total_100s_by_batsman.groupby(['match_id','season','batsman'])['batsman_runs'].sum().reset_index()
    total_100s_by_batsman = total_100s_by_batsman.rename(columns={'batsman_runs':'number_of_100s'})
    total_100s_by_batsman['number_of_100s'] = (total_100s_by_batsman.number_of_100s>=100)
    total_100s_by_batsman['number_of_100s'] = total_100s_by_batsman['number_of_100s'].astype(int)
    total_100s_by_batsman = total_100s_by_batsman.groupby(['season','batsman'])['number_of_100s'].sum().reset_index()
    total_100s_by_batsman.sort_values(ascending=False,by='number_of_100s',inplace=True) 
    
    # number of 50s season wise
    total_50s_by_batsman = df
    total_50s_by_batsman = total_50s_by_batsman.groupby(['match_id','season','batsman'])['batsman_runs'].sum().reset_index()
    total_50s_by_batsman = total_50s_by_batsman.rename(columns={'batsman_runs':'number_of_50s'})
    total_50s_by_batsman['number_of_50s'] = ((total_50s_by_batsman.number_of_50s>=50)&(total_50s_by_batsman.number_of_50s<100))
    total_50s_by_batsman['number_of_50s'] = total_50s_by_batsman['number_of_50s'].astype(int)
    total_50s_by_batsman = total_50s_by_batsman.groupby(['season','batsman'])['number_of_50s'].sum().reset_index()
    total_50s_by_batsman.sort_values(ascending=False,by='number_of_50s',inplace=True) 
    
    # number of 6s by player
    number_of_6s_by_player = df[df.batsman_runs==6].groupby(['season','batsman'])['batsman_runs'].count().reset_index()
    number_of_6s_by_player.rename(columns = {'batsman_runs':'number_of_6s'},inplace=True)
    number_of_6s_by_player.sort_values(ascending=False,by='number_of_6s',inplace=True) 
    
    # number of 4s by player
    number_of_4s_by_player = df[df.batsman_runs==4].groupby(['season','batsman'])['batsman_runs'].count().reset_index()
    number_of_4s_by_player.rename(columns = {'batsman_runs':'number_of_4s'},inplace=True)
    number_of_4s_by_player.sort_values(ascending=False,by='number_of_4s',inplace=True) 
    
    # Orange Cap Table
    orange_cap_table = orange_cap_season_wise.merge(matches_played,left_on=['season','batsman'],right_on=['season','batsman'],how='left')
    orange_cap_table = orange_cap_table.merge(highest_score_per_season,left_on=['season','batsman'],right_on=['season','batsman'],how='left')
    orange_cap_table.rename(columns={('highest_score', ''):'highest_score'},inplace=True)
    orange_cap_table = orange_cap_table.merge(total_100s_by_batsman,left_on=['season','batsman'],right_on=['season','batsman'],how='left')
    orange_cap_table = orange_cap_table.merge(total_50s_by_batsman,left_on=['season','batsman'],right_on=['season','batsman'],how='left')
    orange_cap_table = orange_cap_table.merge(number_of_6s_by_player,left_on=['season','batsman'],right_on=['season','batsman'],how='left')
    orange_cap_table = orange_cap_table.merge(number_of_4s_by_player,left_on=['season','batsman'],right_on=['season','batsman'],how='left')
    
    # dataframe convert to json formate
    df1 = orange_cap_table
    record = []
    for index,row in df1.iterrows():
        rec = []
        rec.append(row[0])
        rec.append(row[1])
        rec.append(row[2])
        rec.append(row[3])
        rec.append(row[4])
        rec.append(row[5])
        rec.append(row[6])
        rec.append(row[7])
        rec.append(row[8])
        record.append(rec)
    
    s = "var orange_cap_table_data = "+str(record)
    
    return s.strip()



def top10_batsman_of_each_season():

    global runs
    df = runs

    # top 10 batsman of each season
    per_season_runs = df.groupby(['season','batsman'])['batsman_runs'].sum().reset_index()
    per_season_runs.sort_values(ascending=False,by='batsman_runs',inplace=True) 
    top_10_batsman_each_season = per_season_runs.groupby('season').head(10)

    # dataframe convert to json formate
    df = top_10_batsman_each_season

    def get_nested_rec(key, grp):
        rec = {}
        rec['label'] = key[1]
        rec['y'] = int(key[2])
        return rec

    record = {}
    for key, grp in df.groupby(['season','batsman','batsman_runs']):
        if key[0] not in record.keys():
            record[key[0]] = []
        rec = get_nested_rec(key, grp)
        record[key[0]].append(rec)

    
    s = "var top_10_batsman_each_season = "+str(record)

    return s.strip()


def top_10_six_hitter_each_season():

    global runs
    # number of 6s in each season
    per_season_6s = runs[runs.batsman_runs==6].groupby(['season','batsman'])['batsman_runs'].count().reset_index()
    per_season_6s.rename(columns = {'batsman_runs':'number_of_6s'},inplace=True)
    # top 10 batsman who hit maximum 6s in each season
    per_season_6s.sort_values(ascending=False,by='number_of_6s',inplace=True) 
    top_10_six_hitter_per_season = per_season_6s.groupby(['season']).head(10)

    # convert dataframe to json formate
    df = top_10_six_hitter_per_season

    def get_nested_rec(key, grp):
        rec = {}
        rec['label'] = key[1]
        rec['y'] = int(key[2])
        return rec

    record = {}
    for key, grp in df.groupby(['season','batsman','number_of_6s']):
        if key[0] not in record.keys():
            record[key[0]] = []
        rec = get_nested_rec(key, grp)
        record[key[0]].append(rec)

    
    s = "var top_10_six_hitter_per_season = "+str(record)
  
    return s.strip()


def top_10_fours_hitter_each_season():

    global runs
    # top 10 most 4s hitter of each ipl season
    per_season_4s = runs[runs.batsman_runs==4].groupby(['season','batsman'])['batsman_runs'].count().reset_index()
    per_season_4s.rename(columns = {'batsman_runs':'number_of_4s'},inplace=True)
    # top 10 batsman who hit maximum 4s in each season
    per_season_4s.sort_values(ascending=False,by='number_of_4s',inplace=True) 
    top_10_fours_hitter_per_season = per_season_4s.groupby(['season']).head(10)

    # convert top_10_fours_hitter_per_season dataframe to json formate
    df = top_10_fours_hitter_per_season

    def get_nested_rec(key, grp):
        rec = {}
        rec['label'] = key[1]
        rec['y'] = int(key[2])
        return rec

    record = {}
    for key, grp in df.groupby(['season','batsman','number_of_4s']):
        if key[0] not in record.keys():
            record[key[0]] = []
        rec = get_nested_rec(key, grp)
        record[key[0]].append(rec)

    
    s = "var top_10_fours_hitter_per_season = "+str(record)

    return s.strip()   

def toss_winners_percentage_of_success():

    global matchdf

    # toss winners percentage of success
    winner = matchdf[matchdf['toss_winner']==matchdf['winner']]
    win = (len(winner) / (matchdf.shape[0]))*100
    loss = ((matchdf.shape[0]-len(winner)) / (matchdf.shape[0]))*100

    record = [{'y': float("{:.2f}".format(win)), 'label':"Win"},{'y': float("{:.2f}".format(loss)), 'label':"Loss"}]
    
    s = "var toss_winners_percentage_of_success = "+str(record)

    return s.strip()
    

# highest score of team
def highest_score_of_team():
    
    global runs
    df = runs
    # highest score of team
    highest_score_of_team = df.groupby(['match_id','batting_team'])['total_runs'].sum().reset_index()
    highest_score_of_team = highest_score_of_team.groupby('batting_team')['total_runs']
    highest_score_of_team = highest_score_of_team.nlargest(1).reset_index()
    highest_score_of_team.drop(['level_1'],axis=1,inplace=True)
    highest_score_of_team.rename(columns={'total_runs':'highest_score'},inplace=True)
    highest_score_of_team.sort_values(ascending=False,by='highest_score',inplace=True)

    # convert dataframe to json formate
    df = highest_score_of_team

    record = []
    for index,row in df.iterrows():
        rec = {}
        team, score = row
        rec['y'] = score
        rec['label']=team
        record.append(rec)

    s = "var highest_score_of_team = "+str(record)

    return s.strip()


# team percentage of matches won out of total matches played
def team_won_percentage():

    global matchdf, deliverydf

    # merge
    df = matchdf.merge(deliverydf, left_on = 'match_id', right_on = 'match_id', how = 'left')

    # team matches played
    team_matches_played = df.groupby(['match_id','batting_team'])['total_runs'].sum().reset_index()
    team_matches_played['count']=1
    team_matches_played = team_matches_played.groupby(['batting_team'])['count'].sum().reset_index()

    # team innings win
    df = matchdf[(matchdf.result==1)|(matchdf.result==2)]
    df['match_wins'] = 1
    team_matches_win = df.groupby(['winner'])['match_wins'].sum().reset_index()
    team_matches_win = team_matches_win.merge(team_matches_played, left_on='winner', right_on='batting_team',how='left')
    team_matches_win.rename(columns={'winner':'team','count':'matches_played'},inplace=True)
    team_matches_win.drop(['batting_team'],axis=1,inplace=True)

    # team win percentage
    team_win_percentage = team_matches_win
    team_win_percentage['win%'] = (team_win_percentage['match_wins']/team_win_percentage['matches_played'])*100
    team_win_percentage.sort_values(ascending=False,by='win%',inplace=True)

    # convert dataframe to json formate
    df = team_win_percentage[['team','win%']] 

    record = []
    for index,row in df.iterrows():
        rec = {}
        team, win = row
        rec['y'] = float("{:.2f}".format(win))
        rec['label']=team
        record.append(rec)

    
    s = "var team_win_percentage = "+str(record)

    return s.strip()



# number of 200+ scoring matches in each season
def number_of_200_plus_scoring_matches():

    global runs
    df = runs
    scores =  df.groupby(['match_id','season','batting_team','bowling_team'])['total_runs'].sum().reset_index() 
    number_of_200_plus_scoring_matches = scores[scores['total_runs']>=200]
    number_of_200_plus_scoring_matches = number_of_200_plus_scoring_matches.groupby(['season'])['match_id'].count().reset_index()
    number_of_200_plus_scoring_matches.rename(columns={'match_id':'count'},inplace=True)
    
    # convert number_of_200_plus_scoring_matches  dataframe to json formate
    df = number_of_200_plus_scoring_matches 

    record = []
    for index,row in df.iterrows():
        rec = {}
        team, count = row
        rec['y'] = count
        rec['label']=team
        record.append(rec)

    s = "var number_of_200_plus_scoring_matches = "+str(record)

    return s.strip()


# top 10 most wkt taker in ipl history
def top_10_most_wkt_taker():

    global runs
    df = runs

    df =df[(df.wicket_kind!='run out')&(df.wicket_kind!='retired hurt')]
    top_10_most_wkt_taker = df.groupby(['bowler'])['wicket'].sum()
    top_10_most_wkt_taker = top_10_most_wkt_taker.nlargest(10).reset_index()
    
     # convert dataframe to json formate
    df = top_10_most_wkt_taker

    record = []
    for index,row in df.iterrows():
        rec = {}
        bowler, wkt = row
        rec['y'] = wkt
        rec['label']=bowler
        record.append(rec)

    s = "var top_10_most_wkt_taker = "+str(record)

    return s.strip()
    
    
# top_10_most_wkt_taker of each season
def top_10_wkt_taker_of_each_season():

    global runs
    df = runs
    
    df =df[(df.wicket_kind!='run out')&(df.wicket_kind!='retired hurt')]
    top_10_wkt_taker_of_each_season = df.groupby(['season','bowler'])['wicket'].sum().reset_index()
    top_10_wkt_taker_of_each_season.sort_values(ascending=False,by='wicket',inplace=True) 
    top_10_wkt_taker_of_each_season = top_10_wkt_taker_of_each_season.groupby('season').head(10)
    
    # dataframe convert to json formate
    df = top_10_wkt_taker_of_each_season

    def get_nested_rec(key, grp):
        rec = {}
        rec['label'] = key[1]
        rec['y'] = int(key[2])
        return rec

    record = {}
    for key, grp in df.groupby(['season','bowler','wicket']):
        if key[0] not in record.keys():
            record[key[0]] = []
        rec = get_nested_rec(key, grp)
        record[key[0]].append(rec)

    s = "var top_10_wkt_taker_of_each_season = "+str(record)

    return s.strip()

# top 10 players with most man of the match awards
def top_10_players_with_most_manofthematch_awards():
    
    global matchdf
    df = matchdf

    top_10_players = pd.DataFrame(df.player_of_match.value_counts()[:10]).reset_index()

    df = top_10_players

    record = []
    for index,row in df.iterrows():
        rec = {}
        player , count = row
        rec['y'] = count
        rec['label']= player
        record.append(rec)

    s = "var top_10_players_with_most_manofthematch_awards = "+str(record)

    return s.strip()

def data_preparation():
    
    data_cleaning()

    s = batsman_table_data_collection() +'\n'+ orangecap_table_data_collection() +'\n'+  top10_batsman_of_each_season() + '\n'+ top_10_six_hitter_each_season() + '\n'+ top_10_fours_hitter_each_season() + '\n'+ toss_winners_percentage_of_success() + '\n'+ highest_score_of_team() + '\n'+ team_won_percentage() + '\n'+ number_of_200_plus_scoring_matches()+'\n' + top_10_most_wkt_taker()+'\n'+ top_10_wkt_taker_of_each_season()+'\n'+top_10_players_with_most_manofthematch_awards()

    # save js file
    with open(PROCESSED_JS_FILES_PATH+'chartdatajs.js','w') as file:
         file.write(s)
    print("Processed Chart Js File Updated!!")

