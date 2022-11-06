import pandas as pd
import numpy as np

from typing import Union
import sqlite3 as sq

# write a path to db file
DB_FILE = '/Users/romanromanov/Documents/GitHub/bets/bets-2/app/bd/latest.db'

class bets_flashscore():
    def __init__(self, db_file = DB_FILE) -> None:
        self.db_file = db_file

    def simple_query_sql(self, sql = "SELECT name FROM sqlite_master WHERE type='table'", data = None):

        conn = sq.connect(self.db_file, timeout=10)

        if 'select' in sql.lower():
            df = pd.read_sql(sql, conn)
            return df

        if 'insert' in sql.lower() and data:
            c = conn.cursor()
            c.executemany(sql, data)
            conn.commit()
            conn.close()
            print('Данные залиты!')
            return

        if 'delete' in sql.lower() or 'update' in sql.lower():
            c = conn.cursor()
            c.execute(sql)
            conn.commit()
            conn.close()
            print('Данные обновлены/удалены !')
            return

def calc_minutes(match_minutes : str) -> int:
    data = []
    for match_minute in match_minutes:
        try:
            data.append(float(match_minute.split(':')[0]) + (float(match_minute.split(':')[1]) / 60))
        except:
            data.append(float(match_minute) / 60)
    return data

def calc_rang_holliger(
                       fgm : int,
                       steals: int,
                       ptm3: int,
                       ftm: int,
                       blocks: int,
                       rebounds_in_attack: int,
                       assists: int,
                       rebounds_in_defence: int,
                       foul: int,
                       ft_miss: int,
                       fg_miss: int,
                       losses: int,
                       time_in_game_minutes: float
                       ):
    """Calculate holliger"""
    return  (fgm * 85.91 +                   #as FGM -- кол-во реализованных бросков
             steals * 53.897 +               #as steals -- перехваты
             ptm3 * 51.757 +                 #as PTM3 -- реализованные 3 очковые
             ftm * 46.845 +                  #as FTM -- реализованные штрафные очки
             blocks * 39.19 +                #as blocks -- реализованные блоки
             rebounds_in_attack * 39.19 +    #as Offensive_Reb -- подборы в атаках
             assists * 34.677 +              #as assists -- результативные передачи
             rebounds_in_defence * 14.707 -  #as Defensive_Reb -- подборы в защитах
             foul * 17.174 -                 #as foul -- фолы
             ft_miss * 20.091 -              #as ft_miss
             fg_miss * 39.19 -               #as fg_miss
             losses * 53.897) * (1 / time_in_game_minutes)

def create_condition(columns : list, values: list) -> str:
    """Create conditions for sql"""

    condition = ''
    for column, value in zip(columns, values):
        if column:
            if isinstance(value, list):
                condition += f' and {column} in ({value})'.replace('[', '').replace(']','')
            if isinstance(value, str):
                condition += f" and {column} = '{value}'"
            if isinstance(value, int) or isinstance(value, float):
                condition += f" and {column} = {value}"
    return condition

def create_window_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    """
    group = (df
            .groupby(['lig_id', 'season_id', 'player_link']).team_name.max()
            .reset_index()
            .sort_values(['lig_id', 'season_id'])
            )

    all_res = pd.DataFrame()
    for team in group.values:
        lig_id, season_id, player_link, team_name = team
        tmp_res = (df[(df.lig_id == lig_id) & 
                      (df.season_id == season_id) & 
                      (df.player_link == player_link)]
            .sort_values(['match_start_date'])
        )
        tmp_res = (tmp_res
            .assign(
                 holl_exp_all_matches = lambda x: x.rang_hollinger.shift(1).expanding().mean()
                # ,holl_rol_last_1_matches = lambda x: x.rang_hollinger.shift(1).rolling(1).mean()
                # ,holl_rol_last_2_matches = lambda x: x.rang_hollinger.shift(1).rolling(2).mean()
                # ,holl_rol_last_3_matches = lambda x: x.rang_hollinger.shift(1).rolling(3).mean()
                # ,holl_rol_last_4_matches = lambda x: x.rang_hollinger.shift(1).rolling(4).mean()
                # ,holl_rol_last_5_matches = lambda x: x.rang_hollinger.shift(1).rolling(5).mean()
            )
        )
        all_res = pd.concat([all_res, tmp_res])
    return all_res

def get_points_for_all_players(result : pd.DataFrame) -> pd.DataFrame:
        """
        Сделаем операции pivot и unpivot чтобы за каждый день у нас были все возможные игроки из команды.
        """
        al = pd.DataFrame()
        for season in result.season_id.unique():
            for team in result.team_name.unique():
                temp_df = result[(result.team_name == team) & (result.season_id == season)]
                temp_df = temp_df.pivot_table(  index = ['lig_id', 'season_id', 'team_name', 'match_start_date']
                                              , columns = ['player_link']
                                              , values = ['holl_exp_all_matches', 
                                                        #   'holl_rol_last_1_matches', 
                                                        #   'holl_rol_last_2_matches',
                                                        #   'holl_rol_last_3_matches',
                                                        #   'holl_rol_last_4_matches',
                                                        #   'holl_rol_last_5_matches'
                                                          ]
                                              , aggfunc = 'max'
                                             ).reset_index()

                temp_df.columns = ['lig_id', 'season_id', 'team_name', 'match_start_date'] + list(temp_df.droplevel(0, axis = 1).columns[4:])
                temp_df = temp_df.melt( id_vars = [ 'lig_id'
                                                  , 'season_id'
                                                  , 'team_name'
                                                  , 'match_start_date']
                                        , value_vars = temp_df.columns[4:]
                                       )
                temp_df.columns = [   'lig_id'
                                    , 'season_id'
                                    , 'team_name'
                                    , 'match_start_date'
                                    , 'player_link'
                                    , 'value'
                            #   , 'holl_exp_all_matches' 
                            #   , 'holl_rol_last_1_matches' 
                            #   , 'holl_rol_last_2_matches'
                            #   , 'holl_rol_last_3_matches'
                            #   , 'holl_rol_last_4_matches'
                            #   , 'holl_rol_last_5_matches'
                            ]
                temp_df.value = temp_df.value.fillna(method='ffill')
                al = pd.concat([al, temp_df]).drop_duplicates()
        return al

def lineups() -> pd.DataFrame:
    """"""

    sql = f"""select distinct 
                          lig_id
                        , season_id
                        , match_start_date
                        , team_name
                        , 1 as ill
                        , player_link 
                  from lineups
              """
    lineups = (bets_flashscore().simple_query_sql(sql)
                    .assign(
                        match_start_date = lambda x: pd.to_datetime(x.match_start_date, dayfirst = True)
                    )
               )
    return lineups


def get_players_statistic_with_holliger(condition : str) -> pd.DataFrame:
    """"""

    bets = bets_flashscore()
    return (bets.simple_query_sql(f"""
                    select * 
                    from players
                    where 1=1
                          {condition}
                """)
        .fillna(0)
        .assign(
              match_start_date = lambda X: pd.to_datetime(X.match_start_date, dayfirst=True)
            , match_h = lambda X: np.where(~X.match_minutes.isin(['0', '0.0', '0:0', '00:00', '0:00']), 
                                           calc_minutes(X.match_minutes), 
                                           0
                                        )
            , rang_hollinger = lambda X: np.where(X.match_h != 0, calc_rang_holliger(
                 X.points_short_all
                ,X.interception
                ,X.points_short_three
                ,X.penalty_points_short
                ,X.blocks_done
                ,X.rebounds_in_attack
                ,X.passing
                ,X.rebounds_in_defence
                ,X.falls
                ,(X.penalty_points_all - X.penalty_points_short)
                ,(X.points_all - X.points_short_all)
                ,X.losses
                ,X.match_h
            ), 0)
        )
    )

def get_finish_data_players(players : pd.DataFrame) -> pd.DataFrame:
    """"""

    players['rank_player'] = players.groupby(['lig_id','season_id','team_name','match_start_date'])['value'].rank(pct = True)    
    players['team_value_holliger'] = players.groupby(['lig_id','season_id','team_name','match_start_date'])['value'].transform('mean')
    players['cc'] = players.sort_values(['lig_id','season_id','team_name','match_start_date','rank_player'], ascending = False).groupby(['lig_id','season_id','team_name','match_start_date']).cumcount()
    players = players[ players.cc <= 9 ]
    players = (
                players
                .pivot_table(index = ['lig_id','season_id','team_name','match_start_date', 'team_value_holliger'],
                             columns = ['cc'],
                             values = ['ill', 'value']
                            )
                .reset_index()
    )
    players['team_value_holliger_rol1'] = players.sort_values(['match_start_date']).groupby(['lig_id','season_id','team_name'])['team_value_holliger'].rolling(1).mean().reset_index(drop=True)
    players['team_value_holliger_rol2'] = players.sort_values(['match_start_date']).groupby(['lig_id','season_id','team_name'])['team_value_holliger'].rolling(2).mean().reset_index(drop=True)
    players['team_value_holliger_rol3'] = players.sort_values(['match_start_date']).groupby(['lig_id','season_id','team_name'])['team_value_holliger'].rolling(3).mean().reset_index(drop=True)
    players['team_value_holliger_rol4'] = players.sort_values(['match_start_date']).groupby(['lig_id','season_id','team_name'])['team_value_holliger'].rolling(4).mean().reset_index(drop=True)
    players['team_value_holliger_rol5'] = players.sort_values(['match_start_date']).groupby(['lig_id','season_id','team_name'])['team_value_holliger'].rolling(5).mean().reset_index(drop=True)

    players.columns = ['lig_id','season_id','team_name','match_start_date', 'team_value_holliger'
                       ,'top_1_ill', 'top_2_ill', 'top_3_ill', 'top_4_ill', 'top_5_ill' ,'top_6_ill', 'top_7_ill', 'top_8_ill', 'top_9_ill', 'top_10_ill'
                       ,'top_1_holliger', 'top_2_holliger', 'top_3_holliger', 'top_4_holliger', 'top_5_holliger','top_6_holliger', 'top_7_holliger', 'top_8_holliger', 'top_9_holliger', 'top_10_holliger'
                       ,'team_value_holliger_rol1', 'team_value_holliger_rol2', 'team_value_holliger_rol3', 'team_value_holliger_rol4', 'team_value_holliger_rol5'
                    ]
    cols = ['lig_id','season_id','team_name','match_start_date'
            ,'team_value_holliger_rol1', 'team_value_holliger_rol2', 'team_value_holliger_rol3', 'team_value_holliger_rol4', 'team_value_holliger_rol5'
            ,'top_1_ill','top_2_ill', 'top_3_ill', 'top_4_ill', 'top_5_ill', 'top_6_ill', 'top_7_ill', 'top_8_ill', 'top_9_ill', 'top_10_ill'
            # ,'team_value_holliger'
        ]
    return (players
                    .assign(
                          top_1_ill = lambda x: np.where(x.top_1_ill == 1, 0, x.top_1_holliger)
                        , top_2_ill = lambda x: np.where(x.top_2_ill == 1, 0, x.top_2_holliger)
                        , top_3_ill = lambda x: np.where(x.top_3_ill == 1, 0, x.top_3_holliger)
                        , top_4_ill = lambda x: np.where(x.top_4_ill == 1, 0, x.top_4_holliger)
                        , top_5_ill = lambda x: np.where(x.top_5_ill == 1, 0, x.top_5_holliger)
                        , top_6_ill = lambda x: np.where(x.top_6_ill == 1, 0, x.top_6_holliger)
                        , top_7_ill = lambda x: np.where(x.top_7_ill == 1, 0, x.top_7_holliger)
                        , top_8_ill = lambda x: np.where(x.top_8_ill == 1, 0, x.top_8_holliger)
                        , top_9_ill = lambda x: np.where(x.top_9_ill == 1, 0, x.top_9_holliger)
                        , top_10_ill = lambda x: np.where(x.top_10_ill == 1, 0, x.top_10_holliger)
                    )

    )[cols]


def pred_conditions(lig_id: int, season_id: list) -> Union[list, list]:
    columns, values = [], []
    if lig_id:
        columns.append('lig_id')
        values.append(lig_id)

    if season_id:
        columns.append('season_id')
        values.append(season_id)
    return columns, values


def calc_holliger(lig_id: int, season_id: list) -> pd.DataFrame:
    """
    Calc Holliger for every game NBA and build top 10 the best players of team by Holliger.
    
    NOW USE ONLY LEAGUE NBA. lig_id : int of number league of basketball. (1 - ACB, 2 - NBA, 3 - euroleague).
    season_id : list of number of seasons. If empty than use all seasons (0,1,2,3)
    """
    # get conditions for sql query
    columns, values = pred_conditions(lig_id=lig_id, season_id=season_id)
    # rename function simple_query_sql for comfortable
    query_sql = bets_flashscore().simple_query_sql
    # calculate for every play and every player holliger ratio 
    players = get_players_statistic_with_holliger(condition = create_condition(columns=columns, values=values))
    # calculate features in during a window time
    prepared_players = create_window_values(players)
    # using pivot and unpivot methods for every game and every players calculate mean holliger
    players_points = get_points_for_all_players(prepared_players)

    # delete players who didn't play for a team
    min_max = (query_sql("select * from players_date_in_team")
        .assign(mn_dt = lambda x: pd.to_datetime(x.mn_dt, yearfirst=True),
                mx_dt = lambda x: pd.to_datetime(x.mx_dt, yearfirst=True)
        )
    )
    players_points = (players_points
        .merge(min_max
                , how= 'left'
                , on = ['lig_id', 'season_id', 'team_name', 'player_link']
                , suffixes=('', '_dt')  
            )
    )
    players = (players_points[(players_points.match_start_date >= players_points.mn_dt) &
                              (players_points.match_start_date <= players_points.mx_dt)]
                .merge(lineups(), 
                       how='left', 
                       on = ['lig_id', 'season_id', 'match_start_date', 'team_name', 'player_link'])
                )
    
    # choose top 10 players by holliger
    players = get_finish_data_players(players = players)
    # final data for model
    condition = create_condition(columns = ['lig_id', 'season_id'], values = [lig_id, list(players.season_id.unique())])
    main_df = (query_sql(
                f"""
                select * from (
                    select lig_id, season_id, match_start_date, team_home_id, team_home_name, team_away_id, team_away_name, team_home_win as team_win, match_home_koef as koef
                    from arch_matches
                    union all
                    select lig_id, season_id, match_start_date, 
                            team_away_id as team_home_id,
                            team_away_name as team_home_name,
                            team_home_id as team_away_id, 
                            team_home_name as team_away_name, 
                            case when team_home_win = 1 then 0 else 1 end as team_win,
                            match_away_koef as koef
                    from arch_matches
                )
                where 1=1
                    {condition}
                """)
                .assign(match_start_date = lambda x: pd.to_datetime(x.match_start_date, dayfirst=True))
                .merge(players, how='left', 
                        left_on=['lig_id', 'season_id', 'match_start_date', 'team_home_name'],
                        right_on=['lig_id', 'season_id', 'match_start_date', 'team_name'],
                    )
                .merge(players, how='left', 
                        left_on=['lig_id', 'season_id', 'match_start_date', 'team_away_name'],
                        right_on=['lig_id', 'season_id', 'match_start_date', 'team_name'],
                        suffixes=('','_away')
                    ) 
                .dropna()
        )
    return main_df

#calc_holliger(lig_id = 2, season_id = [])
#calc_holliger(lig_id = 2, season_id = []).to_excel('for_model.xlsx')