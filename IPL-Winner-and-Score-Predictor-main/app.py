# Importing essential libraries
from flask import Flask, render_template, request
import pickle
from sklearn.externals import joblib 
import numpy as np
from joblib import load
import json 

# Load the score predictor model
# filename = 'model/first-innings-score-lr-model.pkl'
# regressor = pickle.load(open(filename, 'rb'))
filename = "model/score_predictor_model.pkl"
regressor = joblib.load(open(filename, "rb"))

# load the winner predictor model
filename = "model/winner_predictor_model.pkl"
winner_predictor_model = joblib.load(open(filename, "rb"))

team_mapper = {'Sunrisers Hyderabad': 0,
 'Royal Challengers Bangalore': 1,
 'Mumbai Indians': 2,
 'Kolkata Knight Riders': 3,
 'Kings XI Punjab': 4,
 'Chennai Super Kings': 5,
 'Rajasthan Royals': 6,
 'Delhi Capitals': 7}

team_win_percentage = {
 'Sunrisers Hyderabad': 52.100840,
 'Royal Challengers Bangalore': 47.643979,
 'Mumbai Indians': 58.585859,
 'Kolkata Knight Riders': 51.578947,
 'Kings XI Punjab': 46.808511,
 'Chennai Super Kings': 59.090909,
 'Rajasthan Royals': 50.632911,
 'Delhi Capitals': 44.680851
 }


# Opening JSON file 
with open('data/num_of_time_win_match.json') as json_file: 
    number_of_time_win_match = json.load(json_file) 


# function to return key for any value 
def get_key(val, my_dict): 
    for key, value in my_dict.items(): 
         if val == value: 
             return key 

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('Home.html')

@app.route('/winner_predictor', methods=['POST'])
def winner_predictor():
    return render_template('winner_predictor.html')

@app.route('/score_predictor', methods=['POST'])
def score_predictor():
    return render_template('score_predictor.html')

@app.route('/winner_predict', methods=['POST'])
def winner_predict():
    temp_array = list()
    
    if request.method == 'POST':
        
        batting_team = request.form['batting_team']
        bowling_team = request.form['bowling_team']

        set1 = (batting_team, bowling_team)
        set1 = str(set1)
        num_of_time_batting_team_win= number_of_time_win_match[set1]['team1'] 
        num_of_time_bowling_team_win= number_of_time_win_match[set1]['team2'] 

        batting_team_won = team_win_percentage[batting_team]
        bowling_team_won = team_win_percentage[bowling_team]

        bowling_team = team_mapper[bowling_team]
        batting_team = team_mapper[batting_team]

        venue = int(request.form['venue'])

        over, ball = str(float(request.form['over'])).split('.')
        over = int(over)
        ball = int(ball)

        striker = int(request.form['striker'])

        non_striker = int(request.form['non_striker'])

        toss_winner = request.form['toss_winner']
        toss_winner = team_mapper[toss_winner]

        toss_decision = int(request.form['toss_decision'])

        score = int(request.form['score'])

        wickets = int(request.form['wickets'])

        target_score = int(request.form['target_score'])

        projected_score = (score/(over*6+ball))*120

        per_ball_run_require = (target_score-score)/((120-over*6)-(ball-1))

        season = 13

        
        
        temp_array = [batting_team, bowling_team, venue, over, ball, striker, non_striker, toss_winner, toss_decision, score, wickets, target_score, projected_score, per_ball_run_require, season, num_of_time_batting_team_win, num_of_time_bowling_team_win, batting_team_won, bowling_team_won]
        print(temp_array)

        data = np.array([temp_array])

        my_prediction = winner_predictor_model.predict_proba(data)[0]

        m = max(list(my_prediction))

        if list(my_prediction).index(m):

            win = get_key(batting_team, team_mapper)
        else:

            win = get_key(bowling_team, team_mapper)
              
        return render_template('winner_predictor_result.html', prediction = float("{:.2f}".format(m))*100 , team = win)

        

@app.route('/score_predict', methods=['POST'])
def score_predict():
    temp_array = list()
    
    if request.method == 'POST':
        
        batting_team = request.form['batting_team']

        bowling_team = request.form['bowling_team']

        venue = int(request.form['venue'])

        over, ball = str(float(request.form['overs'])).split('.')
        over = int(over)
        ball = int(ball)

        runs = int(request.form['score'])

        wickets = int(request.form['wickets'])
      
        projected_score = (runs / (over*6+ball))*120

        temp_array = [team_mapper[batting_team],team_mapper[bowling_team],venue,over,ball,runs,wickets,projected_score]
        
        data = np.array([temp_array])
        my_prediction = int(regressor.predict(data)[0])
    
    
        max_score = ((120-over*6)-(ball))*6+ runs
        
        if max_score < my_prediction:
            my_prediction = max_score
            lower_limit = my_prediction-5
            upper_limit = my_prediction
        else:
            lower_limit = my_prediction-5
            upper_limit = my_prediction+10

        if lower_limit<0:
            lower_limit = 0

        if upper_limit<0:
            upper_limit=0

        if (over == 19 and ball==6):
            lower_limit = my_prediction
            upper_limit = my_prediction
        elif (over>19):
            lower_limit = runs
            upper_limit = runs
              
        return render_template('score_predictor_result.html', lower_limit = lower_limit , upper_limit =upper_limit )



if __name__ == '__main__':
    app.run(debug=True)
    #app.run(port=8000,debug=True)
