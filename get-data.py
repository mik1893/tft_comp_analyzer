import requests
import json
import pandas as pd
import numpy
from ratelimit import limits, sleep_and_retry

# static headers with Riot Token
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:75.0) Gecko/20100101 Firefox/75.0",
    "Accept-Language": "en-GB,en;q=0.5",
    "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://developer.riotgames.com",
    "X-Riot-Token": "RGAPI-2a3b2879-873b-444f-a6c7-e865074db35b"
}

# Other Settings
MATCH_NR = 20 # number of matches to retrieve
RECURSION_DEGREE = 2 # number of hops across matches (e.g. get all participants of from all the matches of the participants of your matches = 2nd degree)
API_CALL_LIMIT = 100
API_CALL_PERIOD = 120

# QUEUE IDS
RANKED = 1100
NORMAL = 1090


#################### Rate-limited request.get ####################
@sleep_and_retry
@limits(calls=API_CALL_LIMIT, period=API_CALL_PERIOD)
def call_api(url):
    response = requests.get(url,headers=HEADERS)
    if response.status_code != 200:
        raise Exception('API response: {}'.format(response.status_code))
    return response


# Global variables for output
SINERGIES = []
MATCHDATA = []

#################### get list of matches for a set of summoners ####################
def get_data(summoners, degree):
    try:
        degree = degree + 1
        if degree <= RECURSION_DEGREE:
            for summoner in summoners:
                puuid_url = "https://euw1.api.riotgames.com/tft/summoner/v1/summoners/by-name/"+summoner
                puuid = json.loads(call_api(puuid_url).text)['puuid']
                matches_url = "https://europe.api.riotgames.com/tft/match/v1/matches/by-puuid/"+puuid+"/ids?count="+str(MATCH_NR)
                matches = json.loads(call_api(matches_url).text)

                for match in matches:
                    sinergies, matchdata = get_info(match)
                    SINERGIES.append(sinergies)
                    MATCHDATA.append(matchdata)
                    summ_list = matchdata["puuid"].unique().tolist()
                    get_data(summ_list, degree)

    except Exception as e:
        print("GetData - Error: "+str(e))

#################### retrieve sinergy table for every match ####################
def get_info(match):
    try:
        match_url = "https://europe.api.riotgames.com/tft/match/v1/matches/"+match
        match_info = json.loads(call_api(match_url).text)
        sinergies = []
        matchdata = []
        if match_info['info']['queue_id'] == RANKED and match_info['info']['tft_set_number'] == 3 :
            for participant in match_info['info']['participants']:
                # loop through participants and start generating the dataset
                df = pd.json_normalize(participant["traits"])
                df["puuid"] = participant["puuid"]
                df["match"] = match
                sinergies.append(df)
                ptcp = pd.DataFrame(columns=["match","puuid","level","placement","last_round"])
                ptcp.loc[0] = [match,participant["puuid"],participant["level"],participant["placement"],participant["last_round"]]
                matchdata.append(ptcp)

            matchdata = pd.concat(matchdata)
            sinergies = pd.concat(sinergies)
            return sinergies , matchdata    
        else:
            return None, None
    except Exception as e:
        print("GetInfo - Error: "+str(e))


# Main Thread
if  __name__ == "__main__":
    
    # start with one name and 0 degree degree recursion
    get_data(["Holysh3it"],0)

        
    SINERGIES = pd.concat(SINERGIES)
    SINERGIES.reset_index(drop=True, inplace=True)
    SINERGIES.set_index(["match","puuid","name"],inplace=True)
    SINERGIES.to_csv("output/sinergies_data.csv")

    MATCHDATA = pd.concat(MATCHDATA)
    MATCHDATA.reset_index(drop=True, inplace=True)
    # list of players - not needed for analysis
    # pd.DataFrame(MATCHDATA["puuid"].unique()).to_csv("output/players.csv",header=None, index=None)
    MATCHDATA.set_index(["match","puuid"],inplace=True)
    MATCHDATA.to_csv("output/match_data.csv")

    
    pass