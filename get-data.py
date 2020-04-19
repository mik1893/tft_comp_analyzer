import requests
import json
import pandas as pd
import numpy
from datetime import datetime
from ratelimit import limits, RateLimitException
from backoff import on_exception, expo
import os
import os.path as path


# test PUUID: OzD4nE14YJT5v_St4TwN2hLHlAGc0BRKb71cOF16KR9HBWQ_kPxn5b3-oiviPTtVTgCU5peELj69vg
# test ACCOUNT_ID: ZSRwpeKmJYZv19TKT-1--kBIYxNkzr0JtIfsy0Wd6N-cFIF9PewIbAWS

# static headers with Riot Token
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:75.0) Gecko/20100101 Firefox/75.0",
    "Accept-Language": "en-GB,en;q=0.5",
    "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://developer.riotgames.com",
    "X-Riot-Token": "RGAPI-9719e413-3b57-487e-b17e-e7387def6a32"
}

# Other Settings
MATCH_COUNT = 20 # number of matches to retrieve
RECURSION_DEGREE = 2 # number of hops across matches (e.g. get all participants of from all the matches of the participants of your matches = 2nd degree)
API_CALL_LIMIT = 100
API_CALL_PERIOD = 120
SUMMONER_NAME = "Holysh3it"

# QUEUE IDS
RANKED = 1100
NORMAL = 1090


## HTTP call count
COUNT = 0
def increase_counter():
    global COUNT
    COUNT = COUNT + 1
    print(str(COUNT), end="\r", flush=True)

#################### Rate-limited request.get ####################
@on_exception(expo, RateLimitException, max_tries=8)
@limits(calls=API_CALL_LIMIT, period=API_CALL_PERIOD)
def call_api(url):
    response = requests.get(url,headers=HEADERS)
    if response.status_code != 200:
        raise Exception('API response: {}'.format(response.status_code))
    increase_counter()
    return response

#################### clear output folder ####################
def clean_output():
    if path.exists("output/matches.csv"):
        os.remove("output/matches.csv")
    if path.exists("output/items.csv"):    
        os.remove("output/items.csv")
    if path.exists("output/units.csv"):    
        os.remove("output/units.csv")

#################### get list of matches for a set of summoners ####################
def get_data(summoners, degree):
    try:
        # check current degree of recursion
        degree = degree + 1
        if degree <= RECURSION_DEGREE:
            # get last 
            for summoner in summoners:
                matches_url = "https://europe.api.riotgames.com/tft/match/v1/matches/by-puuid/"+summoner+"/ids?count="+str(MATCH_COUNT)
                matches = json.loads(call_api(matches_url).text)

                for match in matches:
                    #print("Reading match "+match+" ...")
                    summ_list = get_info(match)  
                    try:
                        summ_list.remove(summoner)
                    except Exception:
                        #print("Error removing summmoner: "+summoner)
                        pass
                    get_data(summ_list, degree)

    except Exception:
        #print("GetData - Error: "+str(e))
        pass

#################### retrieve info table for every match ####################
def get_info(match):
    try:
        match_url = "https://europe.api.riotgames.com/tft/match/v1/matches/"+match
        match_info = json.loads(call_api(match_url).text)

        # array of dataframes for results
        match_data = []
        unit_data = []
        item_data = []

        # check match conditions
        if match_info['info']['queue_id'] == RANKED and match_info['info']['tft_set_number'] == 3 :
            
            match_datetime = datetime.fromtimestamp(round(match_info['info']['game_datetime']/1000,0))
            for participant in match_info['info']['participants']:
                
                # Items Dataset
                for unit in participant["units"]:
                    if unit["items"]:
                        dfi =  pd.DataFrame(unit["items"])
                        dfi.columns = ["item"]
                        dfi["character_id"] = unit["character_id"]
                        dfi["puuid"] = participant["puuid"]
                        dfi["match"] = match
                        dfi["match_datetime"] = match_datetime
                        item_data.append(dfi)
                
                # Units Dataset
                dfu = pd.json_normalize(participant["units"])
                dfu["puuid"] = participant["puuid"]
                dfu["match"] = match
                dfu["match_datetime"] = match_datetime
                unit_data.append(dfu)

                #Participants Dataset
                dfp = pd.json_normalize(participant["companion"])
                dfp["puuid"] = participant["puuid"]
                dfp["match"] = match
                dfp["match_datetime"] = match_datetime
                dfp["level"] = participant["level"]
                dfp["placement"] = participant["placement"]
                dfp["last_round"] = participant["last_round"]
                dfp["level"] = participant["level"]
                match_data.append(dfp)

            # Save CSVs
            if match_data:
                match_data = pd.concat(match_data)
                summoners = match_data["puuid"].unique().tolist().copy()
                match_data.reset_index(drop=True, inplace=True)
                match_data.set_index(["match","puuid"],inplace=True)
                if path.exists("output/matches.csv"):
                    match_data.to_csv("output/matches.csv", mode='a', header=False, sep='\t')
                else:
                    match_data.to_csv("output/matches.csv", mode='w', sep='\t') # if first time delete previous file and create with header
            if unit_data:
                unit_data = pd.concat(unit_data)
                unit_data.reset_index(drop=True, inplace=True)
                unit_data.set_index(["character_id","match","puuid"],inplace=True)
                if path.exists("output/units.csv"):
                    unit_data.to_csv("output/units.csv", mode='a', header=False, sep='\t')
                else:
                    unit_data.to_csv("output/units.csv", mode='w', sep='\t') # if first time delete previous file and create with header
            if item_data:
                item_data = pd.concat(item_data)
                item_data.reset_index(drop=True, inplace=True)
                item_data.set_index(["character_id","item","match","puuid"],inplace=True)
                if path.exists("output/items.csv"):
                    item_data.to_csv("output/items.csv", mode='a', header=False, sep='\t')
                else:
                    item_data.to_csv("output/items.csv", mode='w', sep='\t') # if first time delete previous file and create with header

            # return list of summoners on which we need to run next iteration
            return summoners
            
        else:
            return None
    except Exception:
        #print("GetInfo - Error: "+str(e))
        return None


# Main Thread
if  __name__ == "__main__":
    
    clean_output()
    # first puuid i need to get via API from summoner name
    puuid_url = "https://euw1.api.riotgames.com/tft/summoner/v1/summoners/by-name/"+SUMMONER_NAME
    summoner = json.loads(call_api(puuid_url).text)['puuid']
    #print(summoner)
    # start with one name and 0 degree recursion
    print("TFT Matches scanned:")
    get_data([summoner],0)

    pass