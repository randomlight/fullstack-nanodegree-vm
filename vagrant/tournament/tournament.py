#!/usr/bin/env python
# 
# tournament.py -- implementation of a Swiss-system tournament
#

import psycopg2


def connect():
    """Connect to the PostgreSQL database.  Returns a database connection."""
    return psycopg2.connect("dbname=tournament")



def deleteMatches():
	"""Remove all the match records from the database."""
	#print "deleteMatches"

	delete_matches_sql = "DELETE FROM MATCH"

	conn = connect()
	cursor = conn.cursor()

	cursor.execute(delete_matches_sql)

	conn.commit()
	conn.close()


def deletePlayers():
	"""Remove all the player records from the database."""
	#print "deletePlayers"

	delete_players_sql = "DELETE FROM player;"

	conn = connect()
	cursor = conn.cursor()

	cursor.execute(delete_players_sql)

	conn.commit()
	conn.close()



def countPlayers():
    """Returns the number of players currently registered."""
    #print "countPlayers"

    count_players_sql = "SELECT COUNT(*) AS player_count FROM player"

    conn = connect()
    cursor = conn.cursor()

    cursor.execute(count_players_sql)

    player_count = cursor.fetchall()[0]

    conn.close()

    return player_count[0]


def registerPlayer(name):
    """Adds a player to the tournament database.
  
    The database assigns a unique serial id number for the player.  (This
    should be handled by your SQL database schema, not in your Python code.)
  
    Args:
      name: the player's full name (need not be unique).
    """
    #print "registerPlayer - " + name

    conn = connect()
    cursor = conn.cursor()

    insert_sql = "INSERT INTO PLAYER (name) VALUES (%s);"

    cursor.execute(insert_sql, (name,))

    conn.commit()

    conn.close()



def playerStandings():
	"""Returns a list of the players and their win records, sorted by wins.

	The first entry in the list should be the player in first place, or a player
	tied for first place if there is currently a tie.

	Returns:
	  A list of tuples, each of which contains (id, name, wins, matches):
	    id: the player's unique id (assigned by the database)
	    name: the player's full name (as registered)
	    wins: the number of matches the player has won
	    matches: the number of matches the player has played
	"""

	#print "playerStandings"

	view_wincount_sql = '''		CREATE VIEW WIN_COUNTS AS
									SELECT 	PLAYER.ID AS ID_WINNER,
											COUNT(MATCH.*) AS WIN_COUNT
									FROM	PLAYER LEFT JOIN MATCH
											ON PLAYER.ID = MATCH.ID_WINNER
									GROUP BY 
											PLAYER.ID
						'''

	view_matchcount_sql = '''	CREATE VIEW MATCH_COUNTS AS
									SELECT PLAYER.ID, PLAYER.NAME, COUNT(MATCH.*) AS MATCH_COUNT
									FROM PLAYER LEFT JOIN MATCH ON (PLAYER.ID = MATCH.ID_PLAYER1 OR PLAYER.ID = MATCH.ID_PLAYER2)
									GROUP BY PLAYER.ID
						'''

	player_standing_sql = '''	SELECT
									M.ID,
									M.NAME,
									W.WIN_COUNT,
									M.MATCH_COUNT
								FROM
									MATCH_COUNTS AS M,
									WIN_COUNTS AS W 
								WHERE
									W.ID_WINNER = M.ID
								ORDER BY
									WIN_COUNT
	                        '''

	conn = connect()
	cursor = conn.cursor()

	cursor.execute(view_wincount_sql)
	cursor.execute(view_matchcount_sql)
	cursor.execute(player_standing_sql)

	player_standing_tuple = cursor.fetchall()

	conn.close()

	return player_standing_tuple



def reportMatch(winner, loser):
    """Records the outcome of a single match between two players.

    Args:
      winner:  the id number of the player who won
      loser:  the id number of the player who lost
    """

    #print "reportMatch"

    record_match_sql = "INSERT INTO match (id_player1, id_player2, id_winner) VALUES (%s, %s, %s)"

    conn = connect()
    cursor = conn.cursor()

    cursor.execute(record_match_sql, (winner, loser, winner,));

    conn.commit()
    conn.close()


 
 
def swissPairings():
    """Returns a list of pairs of players for the next round of a match.
  
    Assuming that there are an even number of players registered, each player
    appears exactly once in the pairings.  Each player is paired with another
    player with an equal or nearly-equal win record, that is, a player adjacent
    to him or her in the standings.
  
    Returns:
      A list of tuples, each of which contains (id1, name1, id2, name2)
        id1: the first player's unique id
        name1: the first player's name
        id2: the second player's unique id
        name2: the second player's name
    """
    #print "swissPairings"

    player_standings_tuple = playerStandings()

    player_count = countPlayers()

    player_match = []

    i = 0

    while (i < player_count):

    	player1 = player_standings_tuple[i]
    	id1 = player1[0]
    	name1 = player1[1]

    	i = i + 1

    	player2 = player_standings_tuple[i]
    	id2 = player2[0]
    	name2 = player2[1]

    	i = i + 1

    	player_match.append((id1, name1, id2, name2))

    return player_match

