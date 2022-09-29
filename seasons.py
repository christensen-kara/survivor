import season_data
import pandas as pd
from sqlalchemy import create_engine
import psycopg2


# DEFINE THE DATABASE CREDENTIALS
### If you wish to use this program, update these to a Postgresql database that you have access to
user = 'USERNAME'
password = 'PASSWORD'
host = 'HOST'
port = 'PORT'
database = 'DATABASE NAME'

# Data was added manually, and contains each seasons number, name, a link to its Wikipedia page, the name of the merge tribe, and finally the indices of the relevant tables
#	on the Wikipedia page, which change due to inconsistant formatting across pages. The final value is the number of rows to skip for the reunion episode on the season 
#	summary table
seasons = [
	[1, 'Survivor: Borneo', 'https://en.wikipedia.org/wiki/Survivor:_Borneo', 'Rattana', 3, 2, 4, 1, 5, 2]
	,[2, 'Survivor: The Austrailian Outback', 'https://en.wikipedia.org/wiki/Survivor:_The_Australian_Outback', 'Barramundi', 3, 2, 5, 1, 6, 1]
	,[3, 'Survivor: Africa', 'https://en.wikipedia.org/wiki/Survivor:_Africa', 'Moto Maji', 3, 2, 5, 1, 6, 1]
	,[4, 'Survivor: Marquesas', 'https://en.wikipedia.org/wiki/Survivor:_Marquesas', 'Soliantu', 3, 2, 5, 1, 6, 1]
	,[5, 'Survivor: Thailand', 'https://en.wikipedia.org/wiki/Survivor:_Thailand', 'Chuay Jai', 3, 2, 5, 1, 6, 1]
	,[6, 'Survivor: The Amazon', 'https://en.wikipedia.org/wiki/Survivor:_The_Amazon', 'Jacaré', 3, 2, 5, 1, 6, 1]
	,[7, 'Survivor: Pearl Islands', 'https://en.wikipedia.org/wiki/Survivor:_Pearl_Islands', 'Balboa', 3, 2, 5, 1, 8, 1]
	,[8, 'Survivor: All-Stars', 'https://en.wikipedia.org/wiki/Survivor:_All-Stars', 'Chaboga Mogo', 3, 2, 5, 1, 6, 1]
	,[9, 'Survivor: Vanuatu', 'https://en.wikipedia.org/wiki/Survivor:_Vanuatu', 'Alinta', 3, 2, 5, 1, 6, 1]
	,[10, 'Survivor: Palau', 'https://en.wikipedia.org/wiki/Survivor:_Palau', 'Koror', 3, 2, 5, 1, 6, 1]
	,[11, 'Survivor: Guatemala', 'https://en.wikipedia.org/wiki/Survivor:_Guatemala', 'Xhakúm', 3, 2, 5, 1, 6, 1]
	,[12, 'Survivor: Panama', 'https://en.wikipedia.org/wiki/Survivor:_Panama', 'Gitanos', 3, 2, 5, 1, 6, 1]
	,[13, 'Survivor: Cook Islands', 'https://en.wikipedia.org/wiki/Survivor:_Cook_Islands', 'Aitutonga', 3, 2, 4, 1, 6, 1]
	,[14, 'Survivor: Fiji', 'https://en.wikipedia.org/wiki/Survivor:_Fiji', 'Bula Bula', 3, 2, 5, 1, 6, 1]
	,[15, 'Survivor: China', 'https://en.wikipedia.org/wiki/Survivor:_China', 'Hae Da Fung', 3, 2, 5, 1, 6, 1]
	,[16, 'Survivor: Micronesia — Fans vs. Favorites', 'https://en.wikipedia.org/wiki/Survivor:_Micronesia', 'Dabu', 3, 2, 5, 1, 6, 1]
	,[17, 'Survivor: Gabon', 'https://en.wikipedia.org/wiki/Survivor:_Gabon', 'Nobag', 3, 2, 5, 1, 6, 1]
	,[18, 'Survivor: Tocanins', 'https://en.wikipedia.org/wiki/Survivor:_Tocantins', 'Forza', 3, 2, 5, 1, 6, 1]
	,[19, 'Survivor: Samoa', 'https://en.wikipedia.org/wiki/Survivor:_Samoa', 'Aiga', 3, 2, 4, 1, 5, 1]
	,[20, 'Survivor: Heros vs. Villians', 'https://en.wikipedia.org/wiki/Survivor:_Heroes_vs._Villains', 'Yin Yang', 3, 2, 5, 1, 6, 1]
	,[21, 'Survivor: Nicaragua', 'https://en.wikipedia.org/wiki/Survivor:_Nicaragua', 'Libertad', 3, 2, 5, 1, 6, 1]
	,[22, 'Survivor: Redemption Island', 'https://en.wikipedia.org/wiki/Survivor:_Redemption_Island', 'Murlonio', 3, 2, 5, 1, 6, 1]
	,[23, 'Survivor: South Pacific', 'https://en.wikipedia.org/wiki/Survivor:_South_Pacific', 'Te Tuna', 3, 2, 4, 1, 5, 1]
	,[24, 'Survivor: One World', 'https://en.wikipedia.org/wiki/Survivor:_One_World', 'Tikiano', 3, 2, 5, 1, 6, 1]
	,[25, 'Survivor: Philippines', 'https://en.wikipedia.org/wiki/Survivor:_Philippines', 'Dangrayne', 3, 2, 5, 1, 6, 1]
	,[26, 'Survivor: Caramoan', 'https://en.wikipedia.org/wiki/Survivor:_Caramoan', 'Enil Edam', 3, 2, 5, 1, 6, 1]
	,[27, 'Survivor: Blood vs. Water', 'https://en.wikipedia.org/wiki/Survivor:_Blood_vs._Water', 'Kasama', 3, 2, 5, 1, 6, 1]
	,[28, 'Survivor: Cagayan', 'https://en.wikipedia.org/wiki/Survivor:_Cagayan', 'Solarrion', 3, 2, 5, 1, 6, 1]
	,[29, 'Survivor: San Juan del Sur — Blood vs. Water', 'https://en.wikipedia.org/wiki/Survivor:_San_Juan_del_Sur', 'Huyopa', 3, 2, 5, 1, 6, 1]
	,[30, 'Survivor: Worlds Apart', 'https://en.wikipedia.org/wiki/Survivor:_Worlds_Apart', 'Merica', 3, 2, 4, 1, 5, 1]
	,[31, 'Survivor: Cambodia — Second Chance', 'https://en.wikipedia.org/wiki/Survivor:_Cambodia', 'Orkun', 5, 4, 6, 3, 7, 1]
	,[32, 'Survivor: Kaôh Rōng', 'https://en.wikipedia.org/wiki/Survivor:_Kaôh_Rōng', 'Dara', 4, 3, 6, 2, 7, 1]
	,[33, 'Survivor: Millennials vs. Gen X', 'https://en.wikipedia.org/wiki/Survivor:_Millennials_vs._Gen_X', 'Vinaka', 3, 2, 4, 1, 5, 1]
	,[34, 'Survivor: Game Changers', 'https://en.wikipedia.org/wiki/Survivor:_Game_Changers', 'Maku Maku', 3, 2, 4, 1, 5, 1]
	,[35, 'Survivor: Heroes vs. Healers vs. Hustlers', 'https://en.wikipedia.org/wiki/Survivor:_Heroes_vs._Healers_vs._Hustlers', 'Solewa', 3, 2, 4, 1, 5, 1]
	,[36, 'Survivor: Ghost Island', 'https://en.wikipedia.org/wiki/Survivor:_Ghost_Island', 'Lavita', 3, 2, 4, 1, 6, 2] 
	,[37, 'Survivor: David vs. Goliath', 'https://en.wikipedia.org/wiki/Survivor:_David_vs._Goliath', 'Kalokalo', 3, 2, 4, 1, 5, 1]
	,[38, 'Survivor: Edge of Extinction', 'https://en.wikipedia.org/wiki/Survivor:_Edge_of_Extinction', 'Vata', 3, 2, 4, 1, 5, 1]
	,[39, 'Survivor: Island of the Idols', 'https://en.wikipedia.org/wiki/Survivor:_Island_of_the_Idols', 'Lumuwaku', 3, 2, 4, 1, 5, 1]
	,[40, 'Survivor: Winners at War', 'https://en.wikipedia.org/wiki/Survivor:_Winners_at_War', 'Koru', 3, 2, 4, 1, 5, 1]
	,[41, 'Survivor 41', 'https://en.wikipedia.org/wiki/Survivor_41', 'Viakana', 3, 2, 4, 1, 5, 1]
	,[42, 'Survivor 42', 'https://en.wikipedia.org/wiki/Survivor_42', 'Kula Kula', 3, 2, 4, 1, 5, 1]
]

# The start of compiliation of tables that hold data fro all seasons
columnTitles = ['Season Name', 'Season Number', 'Season Winner', 'Num Days', 'Num People', 'Num Tribe Swaps', 'Num Starting Tribes', 'Num Finalists', 'Num on Jury', 'Reentry Season?']
forSeasonTable = []

allContestants = pd.DataFrame()
allEpisodes = pd.DataFrame()

for season in seasons:
	# Creates DataFrames for the season and uploads them to the RDS
	temp = season_data.SeasonData(season[0], season[1], season[2], season[3], season[4], season[5], season[6], season[7], season[8], season[9])
	currContestants, currEpisodes = temp.makeFinalTables()

	# Add this seasons data to the all seasons tables
	allContestants = pd.concat([allContestants, currContestants])
	allEpisodes = pd.concat([allEpisodes, currEpisodes])

	forSeasonTable.append(str(temp).split(', '))
	print(temp)
	
# Make a DataFrame holding information on each season
finalSeasons = pd.DataFrame(forSeasonTable, columns = ['Season Name', 'Season Number', 'Winner', 'Number of Days', 'Number of Players', 'Number of Tribe Swaps', 'Number of Starting Tribes', 'Number at Final Tribal', 'Number on Jury', 'Has Players Reenter?', 'Has Returning Players?', 'All Returning Players?', 'Number of Returning Players', 'Has Exile Island?', 'Has Redemption Island?', 'Is Blood vs Water?', 'Has Island Game?', 'Has Edge of Extinction?'])

# Upload the information to the RDS server
try:
	engine = create_engine(name_or_url = 'postgresql://' + user + ':' + password + '@' + host + ':' + port + '/' + database)
	print('Connection Successful')

	finalSeasons.to_sql('allSeason', engine, schema = 'overall', if_exists = 'replace', index = False)
	allContestants.to_sql('allContestants', engine, schema = 'overall', if_exists = 'replace', index = False)
	allEpisodes.to_sql('allEpisodes', engine, schema = 'overall', if_exists = 'replace', index = False)

except Exception as ex:
	print('Connection Unsucessful', ex)



