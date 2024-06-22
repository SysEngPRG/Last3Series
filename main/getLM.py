import sys
import os
import aiohttp
import json
import asyncio
import httplib2
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.errors import HttpError
from dataclasses import dataclass
import time

class GsBuild():

    def get_service_sacc():
        creds_json = "confs/GoogleServiseKEY.json"
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds_service = ServiceAccountCredentials.from_json_keyfile_name(creds_json, scopes).authorize(httplib2.Http())
        return build('sheets', 'v4', http=creds_service)

class InvalidTeamID(Exception):
    def __init__(self, id) -> None:
        self.id = id
        self.message = f"Invalid team ID {self.id}"
    def __str__(self) -> str:
        return self.message
    
class InternalAppError(Exception):
    def __init__(self) -> None:
        self.id = id
        self.message = f"Internal error"
    def __str__(self) -> str:
        return self.message
    
@dataclass
class Values:
    src_table: str
    trg_table: str
    src_list: str
    trg_list: str

    def addVal(self, conf):
        self.add = Values
        return self.add
    
class GetSheetID:
    def __init__(self, vals) -> None:
        self.vals = vals  
        self.src_table = self.vals.src_table
        self.trg_table = self.vals.trg_table
        self.src_list = self.vals.src_list
        self.trg_list = self.vals.trg_list

    def getIDs(self):
        ids = GsBuild.get_service_sacc().spreadsheets().values().batchGet(spreadsheetId=self.src_table, ranges=[f'{self.src_list}!B4', f'{self.src_list}!G4']).execute()
        id1 = ids["valueRanges"][0]["values"][0][0]
        id2 = ids["valueRanges"][1]["values"][0][0]
        return id1, id2
    
class SetConfig:
    def set(self, conf) -> None:
        self.conf = conf
        self.create_values = Values
        self.values = self.create_values(src_table = conf["src_tableId"], trg_table = conf["trg_tableId"], src_list = conf["src_list"], trg_list = conf["trg_list"])
        return self.values

class Statuses():
    def __init__(self, message, time) -> None:
        self.val = SetConfig.values
        self.tableId = self.val.trg_table
        self.list = self.val.trg_list
        self.message = [[message]]
        self.time = [[time]]
        self.status_cell = f"{self.list}!A2"
        self.time_cell = f"{self.list}!A3"
        self.body_time = {
                'valueInputOption' : 'RAW',
                'data' : [
                {'range' : f'{self.time_cell}', 'values' : self.time}
                ]
                }
        self.body_status = {
                'valueInputOption' : 'RAW',
                'data' : [
                {'range' : f'{self.status_cell}', 'values' : self.message}
                ]
                }
        
    def push_time(self):
        try:
            self.gs_push =  GsBuild.get_service_sacc().spreadsheets().values().batchUpdate(spreadsheetId=self.tableId, body=self.body_time).execute()
        except HttpError as e:
            print(f"Invalid data format {e.status_code}")
    def push_msg(self):
        try:
            self.gs_push =  GsBuild.get_service_sacc().spreadsheets().values().batchUpdate(spreadsheetId=self.tableId, body=self.body_status).execute()
            self.push_time()   
        except HttpError as e:
            print(f"Invalid data format {e.status_code}")
        

#Create session and request for single team    
class Fetch:
    def __init__(self, id, type):
        self.type = type
        self.id = id
        if type == "team":
            self.url = f"https://api.opendota.com/api/teams/{self.id}"
        else: 
            self.url = f"https://api.opendota.com/api/teams/{self.id}/matches"
       
    async def request(self):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.url) as resp:
                    response = await resp.json()
                    if response == None or response == {'error': 'invalid team id'}:
                        raise InvalidTeamID(self.id)
            return response
        #ADD EXCEPTIONS
        except aiohttp.ClientConnectionError:
            print("Connection error occurred") 
        except aiohttp.ClientResponseError:
            print("Invalid response received")          

#Async request for both team, slice array for last 12 items 
class Prepare:
    def __init__(self, id_L, id_R):
        self.id_L = id_L
        self.id_R = id_R

    async def async_fetch(self):
        req1 = Fetch(self.id_L, 0)
        req2 = Fetch(self.id_R, 0)
        loop = asyncio.get_event_loop()
        res1 = loop.create_task(req1.request())
        res2 = loop.create_task(req2.request())
        await asyncio.gather(res1,res2)
        return res1.result(), res2.result()
      
    def get_maps(self):
        try:
            res2Teams = asyncio.run(self.async_fetch())
        except InvalidTeamID as e:
            timestamp = time.strftime("%H:%M:%S")
            invalidId_msg = Statuses(e.message, f"occured in {timestamp}")
            invalidId_msg.push_msg()
            raise InternalAppError()
        self.last_maps1 = res2Teams[0][0:11]
        self.last_maps2 = res2Teams[1][0:11]
        return self.last_maps1, self.last_maps2

#Group maps by series 
class IsSameGame:
    def __init__(self, f_maps) -> None:
        self.f_maps = f_maps
        self.len_maps = len(f_maps)
        self.game = []
        self.max_diff = 8000
        self.min_diff = 1200

    async def is_game(self):
        matches = []
        for i in range(0,self.len_maps-1):
            time_diff = self.f_maps[i]["start_time"] - self.f_maps[i+1]["start_time"]
            same_oppo = self.f_maps[i]["opposing_team_name"] == self.f_maps[i+1]["opposing_team_name"]
            if time_diff < self.max_diff and time_diff > self.min_diff and same_oppo:
                matches.append(self.f_maps[i])
            else:
                matches.append(self.f_maps[i])
                self.game.append(matches)
                matches = []
        return self.game

#Create and run async task for both team group by match
class GetSeries:
    def __init__(self, id1, id2):
        self.id1 = id1
        self.id2 = id2
        self.both_res = Prepare(self.id1, self.id2).get_maps()
    async def groupByMatch(self):

        gByMatch1 = IsSameGame(self.both_res[0])
        gByMatch2 = IsSameGame(self.both_res[1])
        self.loop = asyncio.get_event_loop()
        self.last1 = self.loop.create_task(gByMatch1.is_game())
        self.last2 = self.loop.create_task(gByMatch2.is_game())
        await asyncio.gather(self.last1,self.last2)
        return self.last1.result(), self.last2.result()

#Create and run async task for get both init team name
class TeamName:
    def __init__(self, id1, id2):
        self.id1 = id1
        self.id2 = id2
        self.team1_req = Fetch(self.id1, "team")
        self.team2_req = Fetch(self.id2, "team")
        self.teamName1 = ""
        self.teamName2 = ""
    async def tm_async(self):
        self.loop = asyncio.get_event_loop()
        self.team1_tsk = self.loop.create_task(self.team1_req.request())
        self.team2_tsk = self.loop.create_task(self.team2_req.request())
        await asyncio.gather(self.team1_tsk,self.team2_tsk)
        self.team1Res = self.team1_tsk.result()
        self.team2Res = self.team2_tsk.result()
        self.teamName1 = self.team1Res["name"]
        self.teamName2 = self.team2Res["name"]
        return self.teamName1, self.teamName2
    
#Calculate score, results and create object with required fields for single team. But include attribute for both teams
class MakeFields:
    def __init__(self, id1, id2):
        self.id1 = id1
        self.id2 = id2
        try:
            self.getTeamNames = asyncio.run(TeamName(self.id1, self.id2).tm_async())
        except InvalidTeamID as e:
            timestamp = time.strftime("%H:%M:%S")
            invalidId_msg = Statuses(e.message, f"occured in {timestamp}")
            invalidId_msg.push_msg()
            raise InternalAppError()
        self.getTheSeries = GetSeries(id1, id2)
        self.matches = asyncio.run(self.getTheSeries.groupByMatch())
        self.matchesLeft = self.matches[0][0:3]
        self.matchesRight = self.matches[1][0:3]

    async def fields(self, series, teamName):
        self.q_series = len(series)
        self.scores = []
        self.mapsDurs = []
        self.leagues = []
        self.team = []
        self.opposit_team = []
        self.results = []
        self.mapsRes = []
        for i in range(0, self.q_series):
            self.q_matches = len(series[i])
            self.scoreleft = 0
            self.scoreright = 0
            self.mapDur = []
            self.mapRes = []
            self.leagues.append(series[i][0]["league_name"])
            self.team.append(teamName)
            self.opposit_team.append(series[i][0]["opposing_team_name"])
            for i2 in range(0, self.q_matches):
                self.mapDur.append(int(series[i][i2]["duration"]/60))
                if series[i][i2]["radiant"] == series[i][i2]["radiant_win"]:
                    self.scoreleft = self.scoreleft + 1
                    self.mapRes.append(f"{i2+1}win")
                else:
                    self.scoreright = self.scoreright + 1
                    self.mapRes.append(f"{i2+1}lose")
                if  i2 == self.q_matches-1:
                    self.mRes = "lose" if self.scoreleft < self.scoreright else "win" if self.scoreleft > self.scoreright else "draw"
                    self.results.append(self.mRes)
                    self.mapsRes.append(self.mapRes)
                    scoreStr = f"{self.scoreleft}-{self.scoreright}"
                    self.scores.append(scoreStr)
                    self.mapsDurs.append(self.mapDur)
        #print(f"SCORE LEFT: {self.scores}", f"MAPS DURATION:  {self.mapsDurs} n/", f"LEAGUES: {self.leagues}", f"TEAM: {self.team}", f"OPPOSING TEAM: {self.opposit_team}")
        return self.team, self.scores, self.opposit_team, self.results, self.leagues, self.mapsRes, self.mapsDurs



class getFields:
    def __init__(self):
        self.ids = GetSheetID.getIDs(SetConfig.values)
        self.field_init = MakeFields(self.ids[0], self.ids[1])
  
        
    async def fields(self):
        self.loop = asyncio.get_event_loop()
        self.field_task_L = self.loop.create_task(self.field_init.fields(self.field_init.matchesLeft, self.field_init.getTeamNames[0]))
        self.field_task_R = self.loop.create_task(self.field_init.fields(self.field_init.matchesRight, self.field_init.getTeamNames[1]))
        await asyncio.gather(self.field_task_L,self.field_task_R)
        return self.field_task_L.result(), self.field_task_R.result()

class DataShape:
    def __init__(self, obj, obj_i) -> None:
        self.bothTeamsObj = obj
        self.trgTeamObj = self.bothTeamsObj[obj_i]       
        self.teamArr = []
        self.obj_i = obj_i
        self.headers = ["team", "score", "oppositing_team", "result", "league", 
           "res_map1", "res_map2", "res_map3", "res_map4", "res_map5",
            "dur_map1", "dur_map2", "dur_map3", "dur_map4", "dur_map5"]  
    async def shaping(self):
        for i in range(0, len(self.trgTeamObj)):
            self.teamArr.append(self.trgTeamObj[i])
        self.transed = [[self.teamArr[j][i] for j in range(len(self.teamArr))] for i in range(len(self.teamArr[0]))]
        self.slice1 = []
        for i in range(len(self.transed)):
            self.slice1.append(self.transed[i][0:5])
        self.slice1.insert(0, self.headers)
        for el in self.trgTeamObj[5]:
            while len(el) < 5:
                el.append("")
        for el in self.trgTeamObj[6]:
            while len(el) < 5:
                el.append("")
        return self.slice1, self.trgTeamObj[5:]
    

class A_shaper:
    def __init__(self) -> None:
        self.bothObjs = asyncio.run(getFields().fields())

    async def async_shape(self, obj):    
        self.sh_loop = asyncio.get_event_loop()
        self.sh_tskL = self.sh_loop.create_task(DataShape(obj, 0).shaping())
        self.sh_tskR = self.sh_loop.create_task(DataShape(obj, 1).shaping())
        await asyncio.gather(self.sh_tskL, self.sh_tskR)
        teamDataLeft = self.sh_tskL.result()
        teamDataRight = self.sh_tskR.result()
        return teamDataLeft, teamDataRight
    def slicer(self):
        slices = asyncio.run(self.async_shape(self.bothObjs))
        left1 = slices[0][0]
        left2 = slices[0][1]
        right1 = slices[1][0]
        right2 = slices[1][1]
        return left1, left2, right1, right2

class Async_update:
    def __init__(self, row, slice1, slice2) -> None:
        self.val = SetConfig.values
        self.list = self.val.trg_list 
        self.tableId = self.val.trg_table
        self.startRow  = row
        self.slice1 = slice1
        self.slice2 = slice2
    async def gsUpdate(self):
        mainInf = {
                'valueInputOption' : 'RAW',
                'data' : [
                {'range' : f'{self.list}!C{self.startRow}', 'values' : self.slice1}
                ]
                }
        mapRes = {
                'valueInputOption' : 'RAW',
                'data' : [
                {'range' : f'{self.list}!H{self.startRow+1}', 'values' : self.slice2[0]}
                ]
                }
        mapDurs = {
                'valueInputOption' : 'RAW',
                'data' : [
                {'range' : f'{self.list}!M{self.startRow+1}', 'values' : self.slice2[1]}
                ]
                }

        sendMain = GsBuild.get_service_sacc().spreadsheets().values().batchUpdate(spreadsheetId=self.tableId, body=mainInf).execute()
        sendMapRes = GsBuild.get_service_sacc().spreadsheets().values().batchUpdate(spreadsheetId=self.tableId, body=mapRes).execute()
        sendMapDur = GsBuild.get_service_sacc().spreadsheets().values().batchUpdate(spreadsheetId=self.tableId, body=mapDurs).execute()
        timestamp = time.strftime("%H:%M:%S")
        success = Statuses("Successfull update in", timestamp)
        success.push_msg()


class Initial:
    def __init__(self) -> None:
        self.data_init = A_shaper()
        self.data = self.data_init.slicer() 

        
#Make async update
async def runner(data):
    
    fullData = data
    leftUpd = Async_update(1, fullData[0], fullData[1])
    rightUpd = Async_update(7, fullData[2], fullData[3])
    upd_loop = asyncio.get_event_loop()
    updateLeft = upd_loop.create_task(leftUpd.gsUpdate())
    updateRight = upd_loop.create_task(rightUpd.gsUpdate())
    await asyncio.gather(updateLeft, updateRight)
    return updateLeft.result(), updateRight.result()

def ids():
    id_s = GetSheetID.getIDs(SetConfig.values)
    return id_s

def run():
    data_ini = Initial()
    data = data_ini.data
    asyncio.run(runner(data))


if __name__ == '__main__': # avoid run on import
    run()
