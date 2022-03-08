from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Match import Match
from Business.Player import Player
from Business.Stadium import Stadium
from psycopg2 import sql


def createTables() -> None:
    conn = None
    try:
        conn = Connector.DBConnector()

        conn.execute("CREATE TABLE Teams(team_id INTEGER NOT NULL PRIMARY KEY,"
                     "CHECK (team_id > 0))")

        conn.execute("CREATE TABLE Stadiums(stadium_id INTEGER,"
                     " capacity INTEGER NOT NULL,"
                     " team_id INTEGER REFERENCES Teams ON DELETE CASCADE,"
                     " PRIMARY KEY (stadium_id),"
                     " FOREIGN KEY (team_id) REFERENCES Teams(team_id) ON DELETE CASCADE,"
                     " UNIQUE (team_id),"
                     " CHECK (capacity > 0),"
                     " CHECK (stadium_id > 0),"
                     " CHECK (team_id > 0))")

        conn.execute("CREATE TABLE Players(player_id INTEGER PRIMARY KEY,"
                     " team_id INTEGER NOT NULL, "
                     " age INTEGER NOT NULL,"
                     " height INTEGER NOT NULL,"
                     " preferred_foot VARCHAR(5) NOT NULL,"
                     " FOREIGN KEY (team_id) REFERENCES Teams(team_id) ON DELETE CASCADE,"
                     " CHECK (age > 0),"
                     " CHECK (player_id > 0),"
                     " CHECK (team_id > 0),"
                     " CHECK (height > 0),"
                     " CHECK (preferred_foot IN ('Left', 'Right')))")

        conn.execute("CREATE TABLE Matches(match_id INTEGER PRIMARY KEY NOT NULL,"
                     " competition VARCHAR(13) NOT NULL,"
                     " first_team_id INTEGER NOT NULL REFERENCES Teams(team_id) ON DELETE CASCADE,"
                     " second_team_id INTEGER NOT NULL REFERENCES Teams(team_id) ON DELETE CASCADE,"
                     " CHECK (first_team_id<>second_team_id),"
                     " CHECK (competition IN ('International', 'Domestic')),"
                     " CHECK (match_id > 0),"
                     " CHECK (first_team_id > 0),"
                     " CHECK (second_team_id > 0))")

        conn.execute("CREATE TABLE Player_Scored_In(player_id INTEGER NOT NULL,"
                     " match_id INTEGER NOT NULL,"
                     " num_of_goals INTEGER NOT NULL,"
                     " FOREIGN KEY (player_id) REFERENCES Players(player_id) ON DELETE CASCADE,"
                     " FOREIGN KEY (match_id) REFERENCES Matches(match_id) ON DELETE CASCADE,"
                     " CHECK (num_of_goals > 0),"
                     " PRIMARY KEY (player_id, match_id))")

        conn.execute("CREATE TABLE Played_In(match_id INTEGER NOT NULL,"
                     " stadium_id INTEGER NOT NULL,"
                     " audience_number INTEGER NOT NULL,"
                     " FOREIGN KEY (match_id) REFERENCES Matches(match_id) ON DELETE CASCADE,"
                     " FOREIGN KEY (stadium_id) REFERENCES Stadiums(stadium_id) ON DELETE CASCADE,"
                     " PRIMARY KEY (match_id),"
                     " CHECK(audience_number > -1))")

        conn.execute("CREATE VIEW Goals_Per_Match AS "
                     " SELECT match_id, SUM(num_of_goals) "
                     " FROM Player_Scored_In "
                     " GROUP BY match_id ")

        conn.execute("CREATE VIEW Played_At_Least_One_Match AS "
                     " SELECT DISTINCT T.team_id "
                     " FROM Teams T, Matches M "
                     " WHERE T.team_id = M.first_team_id or T.team_id = M.second_team_id")

        conn.execute("CREATE VIEW Played_At_Least_One_Home_Match AS "
                     " SELECT M.first_team_id, P.audience_number "
                     " FROM  Matches M INNER JOIN Played_In P "
                     " ON P.match_id = M.match_id")

        conn.execute("CREATE VIEW TallTeams AS "
                     " SELECT DISTINCT P1.team_id "
                     " FROM Players P1 INNER JOIN Players P2 ON P1.team_id = P2.team_id" 
                     " WHERE P1.player_id<>P2.player_id and P1.height > 190 and P2.height > 190")

        conn.execute("CREATE VIEW ActiveTallTeams AS "
                     " SELECT DISTINCT P1.team_id "
                     " FROM TallTeams T1 INNER JOIN Played_At_Least_One_Match P1 ON T1.team_id = P1.team_id ")

        conn.execute("CREATE VIEW HomeDidntHaveFortyAudience AS "
                     " SELECT DISTINCT first_team_id "
                     " FROM Played_At_Least_One_Home_Match "
                     "  WHERE NOT (audience_number > 40000) ")

        conn.execute("CREATE VIEW DidntPlayAtHome AS "
                     " SELECT DISTINCT T1.team_id "
                     " FROM Teams T1 LEFT OUTER JOIN Matches P1 "
                     " ON T1.team_id = P1.first_team_id "
                     "  WHERE P1.first_team_id is NULL ")
                    
        conn.execute("""
                     CREATE VIEW GoalsInMatch AS
                     SELECT match_id, SUM(num_of_goals) AS goals
                     FROM Player_Scored_In
                     GROUP BY match_id
                     """)
        
        conn.execute("""
                     CREATE VIEW GoalsInStadium AS
                     SELECT stadium_id, SUM(goals) AS goals
                     FROM Played_In, GoalsInMatch
                     WHERE Played_In.match_id = GoalsInMatch.match_id
                     GROUP BY stadium_id
                     """)

        conn.execute("CREATE VIEW PopularNotEmptyWay AS "
                     "  SELECT P1.first_team_id "
                     "  FROM Played_At_Least_One_Home_Match P1 LEFT OUTER JOIN HomeDidntHaveFortyAudience D1 "
                     "  ON P1.first_team_id = D1.first_team_id "
                     "  WHERE D1.first_team_id IS NULL")
        
        conn.execute("""
                    CREATE VIEW PlayerGoalsInTeam AS
                    SELECT player_id, team_id, COALESCE(total_amount, 0) AS goals
                    FROM (
                        SELECT player_id, team_id FROM Players
                    ) AS PlayerTeam LEFT JOIN (
                        SELECT player_id, SUM(num_of_goals) as total_amount FROM Player_Scored_In GROUP BY player_id
                    ) AS PlayerGoals USING (player_id)
                    """)

    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    pass


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        list_of_tables = ["Played_In", "Player_Scored_In", "Stadiums", "Matches", "Players", "Teams"]
        for table in list_of_tables:
            conn.execute(f"DELETE FROM {table}")
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        list_of_tables = ["Played_In", "Player_Scored_In", "Stadiums", "Matches", "Players", "Teams"]
        list_of_views = reversed(["Goals_Per_Match", "Played_At_Least_One_Match", "Played_At_Least_One_Home_Match",
          "TallTeams", "ActiveTallTeams", "HomeDidntHaveFortyAudience", "DidntPlayAtHome", "GoalsInMatch", "GoalsInStadium",
           "PopularNotEmptyWay", "PlayerGoalsInTeam"])
        for view in list_of_views:
            conn.execute(f"DROP VIEW IF EXISTS {view} CASCADE")
        for table in list_of_tables:
            conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()


def addTeam(teamID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Teams(team_id) VALUES({team_id})").format(team_id=sql.Literal(teamID))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        conn.close()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.close()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.close()
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        conn.close()
        return ReturnValue.ERROR
    conn.close()
    return ReturnValue.OK

def addMatch(match: Match) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Matches(match_id, competition, first_team_id, second_team_id) "
                        "VALUES({match_id}, {competition}, {first_team_id}, {second_team_id})") \
            .format(match_id=sql.Literal(match.getMatchID()), competition=sql.Literal(match.getCompetition()),
                    first_team_id=sql.Literal(match.getHomeTeamID()), second_team_id=sql.Literal(match.getAwayTeamID()))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def getMatchProfile(matchID: int) -> Match:
    conn = None
    try:
        conn = Connector.DBConnector()
        match_getting_query = sql.SQL("SELECT * FROM Matches WHERE match_id = {id_of_match}"). \
            format(id_of_match=sql.Literal(matchID))
        rows_effected, result = conn.execute(match_getting_query)
        if rows_effected != 0:
            return Match(result[0]["match_id"], result[0]["competition"], result[0]["first_team_id"],
                         result[0]["second_team_id"])
        else:
            return Match.badMatch()
        # rows_effected is the number of rows received by the SELECT
    except Exception as e:
        return Match.badMatch()
    finally:
        conn.close()


def deleteMatch(match: Match) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        match_deleting_query = sql.SQL("DELETE FROM Matches WHERE match_id = {id_of_match}").\
            format(id_of_match=sql.Literal(match.getMatchID()))
        rows_effected, _ = conn.execute(match_deleting_query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK
    pass


def addPlayer(player: Player) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Players(player_id, team_id, age, height, preferred_foot) VALUES({player_id},"
                        " {team_id}, {age}, {height}, {preferred_foot})") \
            .format(player_id=sql.Literal(player.getPlayerID()), team_id=sql.Literal(player.getTeamID()),
                    age=sql.Literal(player.getAge()), height=sql.Literal(player.getHeight()),
                    preferred_foot=sql.Literal(player.getFoot()))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def getPlayerProfile(playerID: int) -> Player:
    conn = None
    try:
        conn = Connector.DBConnector()
        match_getting_query = sql.SQL("SELECT * FROM Players WHERE player_id = {id_of_player}"). \
            format(id_of_player=sql.Literal(playerID))
        rows_effected, result = conn.execute(match_getting_query)
        if rows_effected != 0:
            return Player(result[0]["player_id"], result[0]["team_id"], result[0]["age"],
                          result[0]["height"], result[0]["preferred_foot"])
        else:
            return Player.badPlayer()
        # rows_effected is the number of rows received by the SELECT
    except Exception as e:
        return Player.badPlayer()
    finally:
        conn.close()



def deletePlayer(player: Player) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        match_deleting_query = sql.SQL("DELETE FROM Players WHERE player_id = {id_of_player}").\
            format(id_of_player=sql.Literal(player.getPlayerID()))
        rows_effected, _ = conn.execute(match_deleting_query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK
    pass


def addStadium(stadium: Stadium) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Stadiums(stadium_id, capacity, team_id) VALUES({stadium_id}, {capacity},"
                        " {team_id})") \
            .format(stadium_id=sql.Literal(stadium.getStadiumID()), capacity=sql.Literal(stadium.getCapacity()),
                    team_id=sql.Literal(stadium.getBelongsTo()))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
    finally:
        conn.close()
    return ReturnValue.OK


def getStadiumProfile(stadiumID: int) -> Stadium:
    conn = None
    try:
        conn = Connector.DBConnector()
        stadium_getting_query = sql.SQL("SELECT * FROM Stadiums WHERE stadium_id = {id_of_stadium}"). \
            format(id_of_stadium=sql.Literal(stadiumID))
        rows_effected, result = conn.execute(stadium_getting_query)
        if rows_effected != 0:
            return Stadium(result[0]["stadium_id"], result[0]["capacity"], result[0]["team_id"])
        else:
            return Stadium.badStadium()
        # rows_effected is the number of rows received by the SELECT
    except Exception as e:
        return Stadium.badStadium()
    finally:
        conn.close()


def deleteStadium(stadium: Stadium) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        match_deleting_query = sql.SQL("DELETE FROM Stadiums WHERE stadium_id = {id_of_stadium}").\
            format(id_of_stadium=sql.Literal(stadium.getStadiumID()))
        rows_effected, _ = conn.execute(match_deleting_query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK
    pass


def playerScoredInMatch(match: Match, player: Player, amount: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        # if getPlayerProfile(player.getPlayerID()) == player.badPlayer() or getMatchProfile(match.getMatchID()) ==\
        #         match.badMatch():
        #     return ReturnValue.NOT_EXISTS
        query = sql.SQL("INSERT INTO Player_Scored_In(player_id, match_id, num_of_goals) VALUES({player_id},"
                        " {match_id}, {num_of_goals})") \
            .format(player_id=sql.Literal(player.getPlayerID()), match_id=sql.Literal(match.getMatchID()),
                    num_of_goals=sql.Literal(amount))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK
    pass


def playerDidntScoreInMatch(match: Match, player: Player) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Player_Scored_In"
                        " WHERE player_id = {player_id} and match_id = {match_id} ") \
            .format(player_id=sql.Literal(player.getPlayerID()), match_id=sql.Literal(match.getMatchID()))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK
    pass


def matchInStadium(match: Match, stadium: Stadium, attendance: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        # if getPlayerProfile(player.getPlayerID()) == player.badPlayer() or getMatchProfile(match.getMatchID()) ==\
        #         match.badMatch():
        #     return ReturnValue.NOT_EXISTS
        query = sql.SQL("INSERT INTO Played_In(match_id, stadium_id, audience_number) VALUES({match_id},"
                        " {stadium_id}, {audience_number})") \
            .format(match_id=sql.Literal(match.getMatchID()), stadium_id=sql.Literal(stadium.getStadiumID()),
                    audience_number=sql.Literal(attendance))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK
    pass


def matchNotInStadium(match: Match, stadium: Stadium) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Played_In"
                        " WHERE match_id = {match_id} and stadium_id = {stadium_id} ") \
            .format(stadium_id=sql.Literal(stadium.getStadiumID()), match_id=sql.Literal(match.getMatchID()))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK
    pass


def averageAttendanceInStadium(stadiumID: int) -> float:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT COALESCE(AVG(audience_number),0) AS avg_to_generate "
                        "FROM Played_In"
                        " WHERE stadium_id = {stadium_id}").format(stadium_id=sql.Literal(stadiumID))
        rows_effected, result = conn.execute(query)
        if rows_effected != 0:
            return result[0]['avg_to_generate']
    except FloatingPointError:
        return float(0)
    except DatabaseException:
        return float(-1)
    finally:
        conn.close()
    return float(0)


def stadiumTotalGoals(stadiumID: int) -> int:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT  COALESCE(SUM(num_of_goals),0) AS sum_of_goals "
                        "FROM Player_Scored_In M, Played_In P"
                        " WHERE M.match_id = P.match_id and P.stadium_id = {stadium_id}")\
            .format(stadium_id=sql.Literal(stadiumID))
        rows_effected, result = conn.execute(query)
        if rows_effected != 0:
            return result[0]['sum_of_goals']
        elif rows_effected == 0:
            return 0
    except Exception as e:
        return -1
    finally:
        conn.close()


def playerIsWinner(playerID: int, matchID: int) -> bool:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT player_id "
                        "FROM Player_Scored_In P INNER JOIN Goals_Per_Match G "
                        " ON P.match_id = G.match_id "
                        " WHERE P.num_of_goals >= CEILING(G.sum)/2 and P.player_id = {player_id} "
                        "  and P.match_id = {match_id} ")\
            .format(match_id=sql.Literal(matchID), player_id=sql.Literal(playerID))
        rows_effected, result = conn.execute(query)
        if rows_effected != 0:
            return True
        elif rows_effected == 0:
            return False
    except Exception as e:
        return False
    finally:
        conn.close()
    pass


def getActiveTallTeams() -> List[int]:
    conn = None
    list_to_return = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT DISTINCT team_id"
                        " FROM ActiveTallTeams "
                        "ORDER BY team_id DESC "
                        " LIMIT 5")
        rows_effected, result = conn.execute(query)
        if rows_effected != 0:
            for row in range(rows_effected):
                list_to_return.append(result[row]['team_id'])
            return list_to_return
        elif rows_effected == 0:
            return list_to_return
    except Exception as e:
        return list_to_return
    finally:
        conn.close()
    pass


def getActiveTallRichTeams() -> List[int]:
    conn = None
    list_to_return = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT A1.team_id"
                        " FROM Stadiums S1 INNER JOIN ActiveTallTeams A1 "
                        "  ON S1.team_id = A1.team_id "
                        "  WHERE S1.capacity > 55000 "
                        "ORDER BY A1.team_id ASC "
                        " LIMIT 5 ")
        rows_effected, result = conn.execute(query)
        if rows_effected != 0:
            for row in range(rows_effected):
                list_to_return.append(result[row]['team_id'])
            return list_to_return
        elif rows_effected == 0:
            return list_to_return
    except Exception as e:
        return list_to_return
    finally:
        conn.close()
    pass


def popularTeams() -> List[int]:
    conn = None
    list_to_return = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT team_id"
                        " FROM DidntPlayAtHome "
                        "UNION "
                        "   SELECT first_team_id "
                        "   FROM PopularNotEmptyWay "
                        "ORDER BY team_id DESC "
                        " LIMIT 10")
        rows_effected, result = conn.execute(query)
        if rows_effected != 0:
            for row in range(rows_effected):
                list_to_return.append(result[row]['team_id'])
            return list_to_return
        elif rows_effected == 0:
            return list_to_return
    except Exception as e:
        return list_to_return
    finally:
        conn.close()
    pass


def getMostAttractiveStadiums() -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """      
            SELECT stadium_id, COALESCE(goals, 0) AS goals FROM
            (
                GoalsInStadium RIGHT JOIN (
                    SELECT DISTINCT stadium_id FROM Stadiums
                ) AS AllStadiums USING (stadium_id)
            ) ORDER BY goals DESC, stadium_id ASC
            """)
        _, result_set = conn.execute(query)
        if result_set.isEmpty():
            return []
        return [next(iter(row)) for row in result_set.rows]
    except Exception as e:
        return []
    finally:
        conn.close()

def mostGoalsForTeam(teamID: int) -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """      
            SELECT player_id
            FROM PlayerGoalsInTeam
            WHERE team_id = {team_id}
            ORDER BY goals DESC, player_id DESC
            LIMIT 5
            """
        ).format(team_id=sql.Literal(teamID))
        _, result_set = conn.execute(query)
        if result_set.isEmpty():
            return []
        return [next(iter(row)) for row in result_set.rows]
    except Exception as e:
        return []
    finally:
        conn.close()


def getClosePlayers(playerID: int) -> List[int]:
    conn = None
    ret = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL('''
                        SELECT player_id FROM (SELECT g.player_id, COALESCE(COUNT(*), 0) AS scored
                            FROM (SELECT player_id, match_id FROM Player_Scored_In WHERE player_id={player_id}) pm INNER JOIN Player_Scored_In g USING (match_id)
                            WHERE pm.player_id != g.player_id
                            GROUP BY g.player_id) player
                        RIGHT OUTER JOIN 
                        (SELECT player_id FROM Players WHERE player_id != {player_id}) other_players
                        USING (player_id)
                        WHERE 2 * COALESCE(scored, 0) >= (SELECT COUNT(*) FROM Player_Scored_In WHERE player_id={player_id})
                        ORDER BY other_players.player_id
                        ''').format(player_id=sql.Literal(playerID))
        rows_effected, res = conn.execute(query)
    except Exception as e:
        print(e)
        return []
    finally:
        conn.close()

    size = min(10, rows_effected)
    for i in range(size):
        ret.append(res[i]['player_id'])

    return ret