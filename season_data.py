import requests
from bs4 import BeautifulSoup
import unicodedata
from sqlalchemy import create_engine
import psycopg2

import pandas as pd
import numpy as np
import re

# DEFINE THE DATABASE CREDENTIALS
### If you wish to use this program, update these to a Postgresql database that you have access to
user = 'USERNAME'
password = 'PASSWORD'
host = 'HOST'
port = 'PORT'
database = 'DATABASE NAME'


class SeasonData():
	""" Stores and formats data for a Survivor season

		A class used to webscrape data on a Survivor season, and then format it into two tables, one holding data on each episode and one holding data on each contestant. Finally, 
			the data is uploaded to a AWS RDS Database

		Attributes:
			seasonNum: An integer indicating the season's number
			seasonName: A string indicating the season's name
			url: A string indicating the url of the Wikipedia page for the season
			mergeName: A string indicating the name of the merge tribe for the season
			winner: A string indicating the name of the winner of the season
			numDays: An integer indicating the number of days the season lasted
			numPeople: An integer indicating the number of people who played on the season
			numTribeSwaps: An integer indicating the number of times tribes were reshuffled
			numStartingTribes: An integer indicating the number of tribes that players were initially divided into
			numFinalTribal: An integer indicating the number of players who went to final tribal council 
			numJury: An integer indicating the number of players on the jury
			reentrySeason: A boolean indicating if one or more player reentered the game after being eliminated
			hasReturningPlayers: A boolean indicating if one or more player has played in a previous season
			allReturningPlayers: A boolean indicating if all players have played previously
			numReturningPlayers: An integer indicating the number of players who have played previously
			hasExileIsland: A boolean indicating if the season contains the element Exile Island, where most weeks one or more player is sent to a remote island for a portion of the week
			hasRedemptionIsland: A boolean indicating if the season contains the element Redemption Island, where eliminated players have the opprotunity to duel with one another in order to
				win reentry into the game
			isBloodVsWater: A boolean indicating if the season is a Blood vs Water season, where all players begin the game with exactly one loved one on the opposite tribe from them
			hasIslandGame: A boolean indicating if the season contains the element Island Game, where most weeks one or more player is sent to a remote island for a chance to win an
				advantage or earn a punishment
			hasEdgeOfExtinction: A boolean indicating if the season contains the element Edge of Extinction, where eliminated players are sent to another island to live with minimal 
				provisions, until they compete for a chance to return to the game
			episodeIndex: An integer indicating the index of tables that holds the Episodes Table
			seasonSumIndex: An integer indicating the index of tables that holds the Season Summary Table
			voteHistIndex: An integer indicating the index of tables that holds the Voting History Table
			conIndex: An integer indicating the index of tables that holds the Contestants Table
			juryIndex: An integer indicating the index of tables that holds the Jury Table
			numReunionRows: An integer indicating the number of rows of the Season Summary Table dedicated to the reunion episode
			tables: A Beautiful Soup object holding a list of all table elements from the season's Wikipedia page

	"""

	def __init__(self, seasonNum, seasonName, url, mergeName, episodeIndex = 3, seasonSumIndex = 2, voteHistIndex = 4, conIndex = 1, juryIndex = 5, numReunionRows = 1):
		self.seasonNum = seasonNum
		self.seasonName = seasonName
		self.url = url
		self.mergeName = mergeName
		self.winner = ''
		self.numDays = 0
		self.numPeople = 0
		self.numTribeSwaps = 0
		self.numStartingTribes = 0
		self.numFinalTribal = 0
		self.numJury = 0
		self.reentrySeason = False
		self.hasReturningPlayers = False
		self.allReturningPlayers = False 
		self.numReturningPlayers = 0
		self.hasExileIsland = False
		self.hasRedemptionIsland = False
		self.isBloodvsWater = False
		self.hasIslandGame = False
		self.hasEdgeOfExtinction = False

		self.episodeIndex = episodeIndex
		self.seasonSumIndex = seasonSumIndex
		self.voteHistIndex = voteHistIndex
		self.conIndex = conIndex
		self.juryIndex = juryIndex

		self.numReunionRows = numReunionRows

		data = requests.get(self.url)
		html = BeautifulSoup(data.text, 'html.parser')

		self.tables = html.findAll('table')


	def __str__(self):
		return f'{self.seasonName}, {self.seasonNum}, {self.winner}, {self.numDays}, {self.numPeople}, {self.numTribeSwaps}, {self.numStartingTribes}, {self.numFinalTribal}, {self.numJury}, {self.reentrySeason}, {self.hasReturningPlayers}, {self.allReturningPlayers}, {self.numReturningPlayers}, {self.hasExileIsland}, {self.hasRedemptionIsland}, {self.isBloodvsWater}, {self.hasIslandGame}, {self.hasEdgeOfExtinction}'

	def normal(self, x):
		""" Normalizes charaters in the given string

			Takes in an element of any type, if it is a string it normalizes it and otherwise it returns the original element

			Args:
				x: An element

			Returns:
				An element, if x was a string it is the normalized string, otherwise it is the original element

		"""
		if type(x) == str:
			return unicodedata.normalize('NFKD', x)
		else:
			return x

	def unmergeSpan(self, originalTable, funcName):
		"""Expands a webscraped table into cells

			Takes in a Beautiful Soup table element and unmerges all cells with rowspan or columnspan attributes so that all cells are represented in a list of lists 
				format.

			Args:
				originalTable: A Beautiful Soup object holding the table to be unmerged
				functionName: A string indicating the function that unmergeSpan was called inside

			Returns:
				A list of lists holding all the data from the original table, where each cell is an element, each row of the table is a list, and they are all in a 
					larger list
		"""

		multiRowColumns = {}	# Used to keep track of cells from the original table that span one column but multiple rows. Each key in this dictionary will be the index
								#	of the column that spans multiple rows, and the value will be a list where the first entry is the number of rows the column spans and
								#	the second is the value held by the cell
		updatedTable = []
		
		for k in range(len(originalTable)):
			accomodateRowspan = originalTable[k].findAll(['th', 'td'])
			
			if k == 0:
				# During the first row, find the total number of columns in the table, in order to know how many elements each list should have

				totalCells = 0
				for cell in accomodateRowspan:
					try:
						if cell['colspan'] != 1:
							totalCells += int(cell['colspan'])
					except KeyError:
						totalCells += 1

				totalColumns = totalCells

				if self.seasonNum == 36 and funcName == 'vhl': # This voting history table had an extra column in only the first row, so it must be removed
					totalColumns -= 1

			# Reset variables to start a new row
			updatedRow = []
			ogInd = 0

			multiColumnRow = 0	# Used to keep track of cells from the original table that span one row, but multiple columns. multiColumnRow holds the number of columns 
			multiColVal = ''	#	that the cell spans, and multiColVal holds the value of the cell

			for i in range(totalColumns): # Repeats once for each cell in the row
				colspan = False


				if i in multiRowColumns or multiColumnRow != 0: # If the cell in the original table is part of one that spans multiple rows or columns, increase ogInd by
																# 	one, so as to not go out of bounds from the original number of cells in the table
					ogInd += 1



				if multiColumnRow != 0:					# If the current cell is apart of a column that spans multiple rows, append the value that has been saved to the 
					updatedRow.append(multiColVal)		#	updated row, then decrease the variable to indicate of of the cells has been accounted for
					multiColumnRow -= 1

				elif i in multiRowColumns:						# If the current cell is apart of a row that spans multiple columns, append the value that has been saved
					updatedRow.append(multiRowColumns[i][1])	#	to the updated row, then decrease the variable to indicate the cell has been accounted fro
					multiRowColumns[i][0] -= 1

					if multiRowColumns[i][0] == 0:	# If this is zero, the cell has spanned all the rows it spanned in the original table, so it is deleted
						del multiRowColumns[i]

				else:
					try:
						if accomodateRowspan[i - ogInd]['colspan'] != 1:	# Checks if the current cell of the table spans multiple columns by seeing if the colspan attribute
																			#	is not one
							colspan = True

							if accomodateRowspan[i - ogInd].string == '\n' or accomodateRowspan[i - ogInd].string == 'N/A\n': 	# Adjusts variables relevant to spanning many 
								multiColumnRow = int(accomodateRowspan[i - ogInd]['colspan']) - 1 								# 	columns. If there is no string in the 
								multiColVal = '<td>N/A\n</td>'																	#	current cell, adjust it to say N/A. 
								accomodateRowspan[i - ogInd].string = 'N/A\n'													#	Otherwise saves the value of the cell.
							else:
								multiColumnRow = int(accomodateRowspan[i - ogInd]['colspan']) - 1
								multiColVal = str(accomodateRowspan[i - ogInd])
							
					except KeyError: # Indicates that colspan was not an attribute of the current cell
						if accomodateRowspan[i - ogInd].text == '\n':
							accomodateRowspan[i - ogInd].string = 'N/A\n'

					try:
						if accomodateRowspan[i - ogInd]['rowspan'] != 1:	# Checks if the current cell of the table spans multiple columns by seeing if the colspan attribute
																			#	is not one
							if accomodateRowspan[i - ogInd].string == '\n' or accomodateRowspan[i - ogInd].string == 'N/A\n':	# Adjusts variables relavant to spanning many
								multiRowColumns[i] = [int(accomodateRowspan[i - ogInd]['rowspan']) - 1, '<td>N/A\n</td>'] 		#	rows, updating empty cells as necessary
								accomodateRowspan[i - ogInd].string = 'N/A\n'
							else:
								multiRowColumns[i] = [int(accomodateRowspan[i - ogInd]['rowspan']) - 1, str(accomodateRowspan[i - ogInd])] 

							if colspan: # Indicates the current cell contains both a row span and a column span and accounts for this with 
										#	additonal entries to the mulitRowColumn dictionary
								for j in range (1, multiColumnRow + 1):
									if accomodateRowspan[i - ogInd].string == '\n' or accomodateRowspan[i - ogInd].string == 'N/A\n':
										multiRowColumns[i + j] = [int(accomodateRowspan[i - ogInd]['rowspan']) - 1, '<td>N/A\n</td>'] 
										accomodateRowspan[i - ogInd].string = 'N/A\n'
									else:
										multiRowColumns[i + j] = [int(accomodateRowspan[i - ogInd]['rowspan']) - 1, str(accomodateRowspan[i - ogInd])] 


					except KeyError: # Indicates rowspan was not an attribute of the current cell
						if accomodateRowspan[i - ogInd].text == '\n':
							accomodateRowspan[i - ogInd].string = 'N/A\n'

					updatedRow.append(str(accomodateRowspan[i - ogInd]))

			# Replaces and cleans the row before adding it to the unmerged version of the table
			accomodateRowspan = updatedRow 
			accomodateRowspan = [str(i) for i in accomodateRowspan ]
			accomodateRowspan = ''.join(accomodateRowspan)
			accomodateRowspan = BeautifulSoup(accomodateRowspan, 'html.parser')
			updatedTable.append(accomodateRowspan)

		return updatedTable



	def removeExtraSummaryRows(self, cleanedTable):
		"""Removes unnecessary columns in the Season Summary Table

			Takes in the unmerged Season Summary Tables and removes columns holding data unnecessary for this project. Along with this, based on the names of these extra 
				columns it sets various instance variables of the class

			Args:
				cleanedTable: A list of lists holding the unmerged Season Summary Table scraped from Wikipedia

			Returns:
				A list of lists with all elements relating to superfluous columns removed

		"""
		extraRowNames = ['Exile Island', 'Redemption Island', 'Exiled', 'Ghost Island', 'Island of the Idols', 'Decision game', 'Decision game(s)']

		indicesToRemove = []

		for i in range(len(cleanedTable[0])): # Discovers which columns need to be removed by determining which index contains an unwanted column title
			if cleanedTable[0][i] in extraRowNames:
				indicesToRemove.append(i)

			# Uses the column titles to set instance variables

			if not self.hasExileIsland:
				if cleanedTable[0][i] == extraRowNames[0] or cleanedTable[0][i] == extraRowNames[2]:
					self.hasExileIsland = True

			if not self.hasRedemptionIsland:
				if cleanedTable[0][i] == extraRowNames[1]:
					self.hasRedemptionIsland = True

			if not self.hasIslandGame:
				if cleanedTable[0][i] == extraRowNames[3] or cleanedTable[0][i] == extraRowNames[4] or cleanedTable[0][i] == extraRowNames[5] or cleanedTable[0][i] == extraRowNames[6]:
					self.hasIslandGame = True

		indicesToRemove.sort(reverse = True) 

		for i in range(len(indicesToRemove)):	# Remove the necessary indices from the table row by row
			for j in range(len(cleanedTable)):
				cleanedTable[j].remove(cleanedTable[j][indicesToRemove[i]])

		return cleanedTable



	def removeExtraTitleColumns(self, cleanedVotingHistory, cleanedVotesPerTribal, numExtraCols):
		"""Removes extra columns in the Voting History Table

			Takes in the list of lists storing the Voting History Table, and removes the extra rows. This function was necessary because the Voting History Table of
				one season had an unnessary column span for all elements in the first column, which had to be removed

			Args:
				cleanedVotingHistory: A list of lists holding each player of the Seasons Voting History
				cleanedVotesPerTribal: A DataFrame holding the information on the results of each Tribal Council
				numExtraCols: An integer detailing the number of extra columns

			Returns:
				A list of lists holding each player of the Seasons Voting History, with extra columns removed
				A DataFrame holding information on the results of each Tribal Councils, with eextra columns removed
		"""
		fixedVotesPerTribal = cleanedVotesPerTribal.iloc[numExtraCols - 1:] 

		for i in range(len(cleanedVotingHistory)):
			cleanedVotingHistory[i] = cleanedVotingHistory[i][numExtraCols - 1:]

		return fixedVotesPerTribal, cleanedVotingHistory


	def fixSeasonsWithPossesives(self, name):
		"""Removes the name of the person's loved one in a Blood vs Water season

			Takes in the name of the contestant, then sets isBloodvsWater to true if there is a possessive in their name, indicating that they have a loved one. Then, 
				removes the loved one's name using regular expressions. Names without possesives are not modified

			Args:
				name: A string holding the name of the contestant, or in a Blood vs Water season the players name and their relation to their loved one

			Returns:
				A string holding just the player's name

		"""
		if ("\'s " in name or "s\'" in name) and self.isBloodvsWater == False:
			self.isBloodvsWater = True 
		return re.sub(r'[\w.]+\'s [\w]+|[\w.]+s\' [\w]+', "", name)



	def processEpisodeTable(self):
		"""Formats the episode table

			Begins with the Beautiful Soup version of the Episodes Table, cleans it by spliting it into cells and then removing exta ones, finally pairs episode data with
				that episode's description. Finally the results are converted to a DataFrame

			Returns:
				A DataFrame holding information on each episode in the season

		"""
		episodeTable = self.tables[self.episodeIndex]

		rows = episodeTable.findAll('tr')


		descriptions = []

		for i in range(1, len(rows) - 1):
			try:
				if rows[i]['class'][0] == 'vevent': # Determine what the class of the current cell is to determine if it is information on an episode or an episode 
													#	description
					updatedRow = BeautifulSoup(('>|').join(str(rows[i]).split('>')), 'html.parser')

					individualCells = updatedRow.text.split('|')

					individualCells = [i for i in individualCells if i]
					individualCells = [i.replace(u'\xa0', u' ') for i in individualCells]
					
					desiredIndices = [0, 1, 2]	# Some information in the table (such as an episodes rating) is unnessary for this project

					individualCells = [individualCells[i] for i in range(len(individualCells)) if i in desiredIndices]

					try:
						if rows[i + 1]['class'][0] == 'expand-child': 	# Check if the next row contains the episodes description, and add it if so. If not, indicate no 
																		#	description was found
							individualCells.append(rows[i + 1].text)	
						else:
							individualCells.append('None Found')
					except IndexError:
						individualCells.append('None Found')
					except KeyError:
						pass 


					descriptions.append(individualCells)

			except (KeyError, AttributeError):
				pass

		episodes = pd.DataFrame(descriptions, columns = ['Total Episode Number', 'Season Episode Number', 'Episode Name', 'Description'])
		episodes = episodes.applymap(self.normal)


		return episodes


	def processSeasonSummaryTable(self):
		"""Formats the Season Summary Table

			Begins with the Beautiful Soup version of the Season Summary table, then cleans and formats it in a more Python friendly format. Finally, turns the table into a 
				DataFrame

			Returns:
				A DataFrame holding data harvested from the Season Summary Table
		"""
		seasonSummaryBase = self.tables[self.seasonSumIndex]

		seasonSummaryList = seasonSummaryBase.findAll('tr')
		seasonSummaryList = seasonSummaryList[:-self.numReunionRows or None]
		seasonSummaryList = self.unmergeSpan(seasonSummaryList, 'sst')

		finalSeasonSummmary = []

		for episode in seasonSummaryList:

			updatedEpisode = BeautifulSoup((' ').join(str(episode).split('<br/>')), 'html.parser')

			individualCells = updatedEpisode.text.split('\n')
			individualCells = [i for i in individualCells if i]

			finalSeasonSummmary.append(individualCells)

		cleanedSeasonSummary = self.removeExtraSummaryRows(finalSeasonSummmary)[2:]

		try:
			seasonSummary = pd.DataFrame(cleanedSeasonSummary, columns = ['Episode Number', 'Episode Name', 'Air Date', 'Reward Winner', 'Immunity Winner', 'Tribe to Council', 'Eliminated Player'])
		except ValueError: # Some seasons have combined reward and immunity challenges, meaning the number of columns is different than normal and must be dealt with
			combinedRewardImmunity = [22]

			if self.seasonNum in combinedRewardImmunity:
				seasonSummary = pd.DataFrame(cleanedSeasonSummary, columns = ['Episode Number', 'Episode Name', 'Air Date', 'Immunity Winner', 'Tribe to Council', 'Eliminated Player'])

				seasonSummary.insert(3, 'Reward Winner', ['Combined with Immunity' for i in range(len(seasonSummary.index))])

		seasonSummary = seasonSummary.applymap(self.normal)
		return seasonSummary


	def processVotingHistory(self):
		"""Cleans and formats the Voting History Table

			Begins by formatting the data into a list of lists, so each item in a list represents a cell and each list represents a row. Then creates a list of dictionaries,
				where each dictionary corresponds to a vote and contains records of who voted for who. Finally, creates a list of lists indicating the total voting history
				of each person, as well as a list of what each player is called.

			Returns:
				A list of strings holding what each player is called
				A list of lists of strings holding each players voting history
				A list of dictionaries, where each dictionary corresponds to a vote and holds who voted for whom at that vote
		"""
		votingHistoryBase = self.tables[self.voteHistIndex]
		votingHistoryList = votingHistoryBase.findAll('tr')[1:]

		votingHistoryList = self.unmergeSpan(votingHistoryList, 'vhl')

		numELim = len(votingHistoryList[0].findAll(['th', 'td'])) - 1 # Take away one for the title of the row

		votesPerTribal = []
		for i in range(numELim):
			votesPerTribal.append({}) # Each dictionary in the list corresponds to a different vote during the season

		for i in range(len(votingHistoryList)):
			if i != 5 and i != 6: # Skip these rows because they contain no data
				cellsInRow = votingHistoryList[i].findAll(['th', 'td'])
				for j in range(len(votesPerTribal)):	# For each vote of the game, add information from that votes row, including the final vote breakdown and who 
														#	voted for who
					if cellsInRow[j + 1].text.strip() != '' and cellsInRow[j + 1].text.strip() != 'N/A':
						votesPerTribal[j][cellsInRow[0].text.strip()] = cellsInRow[j + 1].text.strip()

		votesPerTribal = pd.DataFrame(votesPerTribal)
		votesPerTribal = votesPerTribal.applymap(self.normal)

		finalVotingHistory = []

		for vote in votingHistoryList:

			updatedVote = BeautifulSoup((' ').join(str(vote).split('<br/>')), 'html.parser')

			individualCells = updatedVote.text.split('\n')
			individualCells = [i for i in individualCells if i]

			finalVotingHistory.append(individualCells)


		extraColspanSeasons = {13:4} # Remove extra colspan in starting column of this season, due to inconsitent formatting on wikipedia page

		if self.seasonNum in extraColspanSeasons:
			votesPerTribal, finalVotingHistory = self.removeExtraTitleColumns(finalVotingHistory, votesPerTribal, extraColspanSeasons[self.seasonNum])

		votesPerTribal = pd.DataFrame(votesPerTribal)
		votesPerTribal = votesPerTribal.applymap(self.normal)

		if self.seasonNum == 13:	# Fix inconsistent name of column from season 13
			votesPerTribal.rename(columns = {'Voting tribe': 'Tribe'}, inplace = True)

		personVotingHistory = finalVotingHistory[7:] # Get only rows containing information on who a player voted for

		called = []
		votingHistories = [] # Save voting histories in a format geared for the contestants table
		for person in personVotingHistory:
			called.append(person[0])

			votes = []
			for vote in person[1:]:
				if vote != 'N/A':
					votes.append(vote)

			votingHistories.append(votes)
			
		votingHistories.reverse()	# Reorder information to correctly add it to contestants table
		called.reverse()

		return called, votingHistories, votesPerTribal


	def processContestantsTable(self, called, votingHistories):
		"""Cleans and formats the Contestants Table

			Begins by turning the Beautiful Soup object into a list of lists, where each inner list holds a row and each element of the list is a cell. Then formats each 
				contestant to account for any past seasons they have played on, and updates the appropriate instance variables. Then, formats each contetstant based on if
				they are eliminated and then reenter the game. Finalley, compiles all the tribes they were on and adds in data from the Voting History table

			Returns:
				A DataFrame holding information on all the contestants from the season

		"""
		contestantBase = self.tables[self.conIndex]

		numTribeSwaps = int(contestantBase.find('th', text = re.compile(r'Tribe', re.DOTALL))['colspan'])

		contestantsTest = [x.text.strip() for x in contestantBase.find('tr').findAll('th')]

		if 'Edge of Extinction' in contestantsTest: # Determines if the season contains Edge of Extinction based on column names
			self.hasEdgeOfExtinction = True

		contestantsList = contestantBase.findAll('tr')[2:]
		contestantsList = self.unmergeSpan(contestantsList, 'ct')

		finalContestants = []
		reenters = {} 	# Holds the names of contestants that are eliminated and then reenter the game, corresponding to a list of lists indicating the places and days
						#	they were eliminated
		for contestant in contestantsList:
			updatedContestant = BeautifulSoup((' ').join(str(contestant).split('<br/>')), 'html.parser')
			returnTest = updatedContestant.find('th').findAll('i')

			trueReturner = False # Indicates if a contestant has actually played the game before

			pastSeasons = []

			if len(returnTest) > 0: # All past seasons are listed in italics, so if there is more than one italicized element in the contestants name, they may have played
									#	before
				for ele in returnTest: 
					returns = ele.findAll('a')	# Past seasons are also links, so the number of links indicates the number of times they have played the game before
					allReturns = len(returns)

					if allReturns > 0 and 'Returned' not in ele: 	# If 'Returned' is in the italics, they indicate a player reentering the game, rather than a player who has
																	#	played a previous season
						if self.hasReturningPlayers == False:
							self.hasReturningPlayers = True
						trueReturner = True

						for pastSeason in returns:	# Save the names of the previous seasons to remove them from the contestants name later
							pastSeasons.append(pastSeason.text.strip())
					else:
						if ele.parent.name == 'a' and 'Returned' not in ele: # Account for one wikipedia page with different formatting than the others

							if self.hasReturningPlayers == False:
								self.hasReturningPlayers = True
							
							trueReturner = True
							allReturns = 1
							pastSeasons.append(ele.text.strip())

			individualCells = updatedContestant.text.split('\n')
			individualCells = [i for i in individualCells if i]

			for pastSeason in pastSeasons:
				individualCells[0] = individualCells[0].replace(pastSeason, '').strip()
			individualCells[0] = individualCells[0].replace('&', '').strip()

			if trueReturner:	# Add cells indicating if a contestant has played before and how many times
				individualCells.append(True)
				individualCells.append(allReturns)

			else:
				individualCells.append(False)
				individualCells.append('N/A')


			if 'Returned' in individualCells[0] or 'Remained' in individualCells[0]: 	# If these words are in the first column, it indicates the contestant was eliminated 
																						#	and then returned to the game
				if len(reenters) == 0:
					self.reentrySeason = True 

				name = individualCells[0].split(' (')[0]

				if name in reenters:	# If the player is already saved as a player that reenters the game, add the new data to their entry. Otherwise their entry is created
					if self.hasRedemptionIsland or self.hasEdgeOfExtinction: 	# Note: Because these seasons have a different number of columns, the indices data is draw from
						reenters[name][0].append(individualCells[-6])			#	are different
						reenters[name][0].append(individualCells[-4])	
					
						reenters[name][1].append(individualCells[-5])
						reenters[name][1].append(individualCells[-3])
					else:
						reenters[name][0].append(individualCells[-4])
						reenters[name][1].append(individualCells[-3])
				else:
					if self.hasRedemptionIsland or self.hasEdgeOfExtinction:
						reenters[name] = [[individualCells[-6], individualCells[-4]], [individualCells[-5], individualCells[-3]]]
					else:
						reenters[name] = [[individualCells[-4]], [individualCells[-3]]]

			else: 	# If 'Returned' or 'Remained' is not in the first column, it indicates the player is truly eliminated from the game at this point. But, we need to check
					# if they have already reentered the game and do the final formatting of the row
				if individualCells[0] in reenters:
					if self.hasRedemptionIsland or self.hasEdgeOfExtinction:
						reenters[individualCells[0]][0].append(individualCells[-6])
						reenters[individualCells[0]][0].append(individualCells[-4])
					
						reenters[individualCells[0]][1].append(individualCells[-5])
						reenters[individualCells[0]][1].append(individualCells[-3])

						individualCells[-6] = reenters[individualCells[0]][0]	
						individualCells[-5] = reenters[individualCells[0]][1]

						for i in range(2):
							individualCells.remove(individualCells[-3])
					else:

						reenters[individualCells[0]][0].append(individualCells[-4])
						reenters[individualCells[0]][1].append(individualCells[-3])

						individualCells[-4] = reenters[individualCells[0]][0]
						individualCells[-3] = reenters[individualCells[0]][1]
				else:

					if self.hasRedemptionIsland or self.hasEdgeOfExtinction:
						individualCells[-6] = [individualCells[-6], individualCells[-4]]
						individualCells[-5] = [individualCells[-5], individualCells[-3]]

						for i in range(2):
							individualCells.remove(individualCells[-3])

					else:
						individualCells[-4] = [individualCells[-4]]
						individualCells[-3] = [individualCells[-3]]

				if individualCells[0] == 'Colton Cumbie': # One player's row was formatted inconsitently, so here it is manually updated
					individualCells = ['Colton Cumbie', '22', 'Collinsville, Alabama', 'Galang', 'N/A', 'N/A', ['Quit', 'N/A'], ['Day 7', 'N/A'],  True, 1]

				finalContestants.append(individualCells)



		startIndex = 3
		endIndex = startIndex + numTribeSwaps

		for j in range(len(finalContestants)):	# Compiles the tribes, which are currently stored one by one in cells, into one list to account for not everyone being on
												#	the same number of tribes
			tribes = []
			for i in range(startIndex, endIndex):
				if finalContestants[j][i] != 'N/A':
					tribes.append(finalContestants[j][i])

			finalContestants[j].append(tribes)
			finalContestants[j] = finalContestants[j][:startIndex] + finalContestants[j][endIndex:] 


		contestants = pd.DataFrame(finalContestants, columns = ['Name', 'Age', 'From', 'Placement', 'Day', 'Returning Player?', 'Num Times Played Before', 'Tribes'])
		contestants = contestants.applymap(self.normal)

		self.numReturningPlayers = contestants['Returning Player?'].sum()

		if self.seasonNum == 27:	# A pair of players has inconsisten formatting, so ehre it is manually updated
			contestants = contestants.drop(labels=9, axis=0)
			contestants.at[8, 'Tribes'] = ['Tadhana', 'Galang', 'Galang']

			contestants.at[16, 'Name'] = 'Ciera Eastin'	

		contestants['Name'] = contestants['Name'].apply(self.fixSeasonsWithPossesives)

		contestants['Called'] = called
		contestants['Voting Histories'] = votingHistories

		return contestants

	def findJury(self, placement):
		"""Determines if a person is on the jury or not

			Args:
				placement: a list of strings corresponding to a contestants placement in the game

			Returns:
				A boolean indicating if the player was on the jury or not

		"""
		for val in placement:
			if 'jury' in val:
				return True
		return False

	
	def addJurytoContestants(self, contestants):
		""" Formats the jury information and adds it to the contestant table

			Cleans data from the jury table, and transforms it into a list of finalists and a dictionary holding jury members and keys and who they voted for of the 
				finalists as values. Then, updates the contestants DataFrame based on the findings from the jury table

			Args:
				contestants: A DataFrame holding data on contestants from the season

			Returns:
				A DataFrame holding data on contestants from the season

		"""
		juryVotesBase = self.tables[self.juryIndex]
		juryVotesList = juryVotesBase.findAll('tr')

		self.numDays = int(juryVotesList[2].findAll('td')[0].text) # Get the number of days buy grabbing the day that final tribal council was on

		juryVotesList = juryVotesList[3:4] + juryVotesList[7:] # Rows 1, 2, 4, 5, and 6 are unnecessary

		# Determine who each jury member voted for by:
		## First, grabbing the names of all the finalists and putting them in a list
		votekey = juryVotesList[0].findAll(['th', 'td']) 

		finalists = []

		for i in range(1, len(votekey)):
			finalists.append(votekey[i].text.strip())

		## Then, based on which column has an image, determining which of the finalists the jury member voted for
		juryVotes = {}

		for i in range(1, len(juryVotesList)):
			currentRow = juryVotesList[i].findAll(['th', 'td'])

			for j in range(1, len(currentRow)):
				if currentRow[j].findAll('img') != []:
					juryVotes[currentRow[0].text.strip()] = finalists[j - 1]

		if self.seasonNum == 36: # Must manually add because in this case Laurel went from a finalist to a jury member to break a tie
			juryVotes['Laurel'] = 'Wendell'


		# Add information based on what was found in the jury table
		contestants['Is On Jury?'] = contestants['Placement'].apply(self.findJury)
		contestants['Is Finalist?'] = contestants['Called'].isin(finalists)
		contestants['Is Winner?'] = contestants['Placement'].apply(lambda x: True if 'Sole Survivor' in x else False)
		contestants['Season Number'] = self.seasonNum
		contestants['Voted For Final Tribal'] = contestants['Called'].apply(lambda x: juryVotes[x] if x in juryVotes else 'N/A')
		contestants['Made Merge?'] = contestants['Tribes'].apply(lambda x: self.mergeName in x)

		self.numFinalTribal = len(finalists)

		return contestants


	def finishEpisodeTable(self, episodeTable, votesPerTribal, seasonSummary):
		"""Combines formatted data into the episodes DataFrame

			Merges the episodes DataFrame with the votes per tribal DataFrame so each episode has record of the vote breakdown for votes that occured during the episode.
				Then merged with the season summary table, so information on who won challenges is included as well. Additional column data is added via calculations.

			Args:
				episodeTable: A DataFrame holding the name of all episodes and their description
				votesPerTribal: A DataFrame holding who voted for who during each vote, as well as the general vote break down and who was eliminated
				seasonSummary: A DataFrame holding who won challenges and was eliminated during each episode

			Returns:
				A DataFrame holding all information related to each episode

		"""
		# Use the episode number to combine data on each episode with the voting information for each episode
		episodesWithTribal = episodeTable.merge(votesPerTribal, left_on = 'Season Episode Number', right_on = 'Episode')

		episodesWithTribal['Eliminated'] = episodesWithTribal['Eliminated'].apply(self.normal)

		# Use Episode number and who was eliminated to add in data on challenge wins for each episode
		episodesWithSeasonSummary = episodesWithTribal.merge(seasonSummary, how = 'left', left_on = ['Season Episode Number', 'Eliminated'], right_on = ['Episode Number', 'Eliminated Player'])

		episodesWithSeasonSummary.bfill(axis ='rows')

		# Find how many eliminations and votes are in each episode. The number of votes may not be equal to the number of eliminations due to ties and revotes. There are
		#	occasionally double elimination episodes, leading to more than one elimination per episode. 
		episodesWithSeasonSummary['Number of Eliminations'] = episodesWithSeasonSummary.groupby(['Season Episode Number'])['Eliminated Player'].transform('nunique')

		try:
			episodesWithSeasonSummary['Number of Votes'] = episodesWithSeasonSummary.groupby(['Season Episode Number', 'Day', 'Tribe'])['Votes'].transform('nunique')
		except KeyError:
			episodesWithSeasonSummary['Number of Votes'] = episodesWithSeasonSummary.groupby(['Season Episode Number', 'Day', 'Tribe'])['Vote'].transform('nunique')

		episodesWithSeasonSummary['Season Number'] = self.seasonNum

		# Standardizes column names
		episodesWithSeasonSummary = episodesWithSeasonSummary.drop(columns = ['Episode Name_x'])
		episodesWithSeasonSummary = episodesWithSeasonSummary.rename(columns = {'Episode Name_y': 'Episode Name'})

		return episodesWithSeasonSummary



	def makeFinalTables(self):
		"""Create tables and update instance variables

			Processes all the tables to create episodes and contestants DataFrames. Updates instance variables based on data in the DataaFrames. Uploads the episodes 
				and contestants tables to a Postgresql server

			Returns:
				A DataFrame holding data on the contestants of the season
				A DataFrame holding data on the episodes of the season

		"""
		# Calls the above functions to make desired DataFrames for the season
		episodeTable = self.processEpisodeTable()
		seasonSummary = self.processSeasonSummaryTable()
		called, votingHistories, votesPerTribal = self.processVotingHistory()
		contestants = self.processContestantsTable(called, votingHistories)
		contestants = self.addJurytoContestants(contestants)
		episodes = self.finishEpisodeTable(episodeTable, votesPerTribal, seasonSummary)

		# Set final variables
		self.winner = contestants[contestants['Is Winner?'] == True]['Name'].values[0]
		self.numPeople = len(contestants.index)

		startingTribes = set()
		tribeSwaps = -np.inf
		for value in contestants['Tribes'].values:
			if len(value) > tribeSwaps:
				tribeSwaps = len(value)
			startingTribes.add(value[0])

		self.numTribeSwaps = tribeSwaps - 1
		self.numStartingTribes = len(startingTribes)
		self.numJury = contestants['Is On Jury?'].sum()

		if self.numReturningPlayers > 0:
			if int(self.numReturningPlayers) == int(self.numPeople):
				self.allReturningPlayers = True

		# Upload the DataFrames to the RDS as PostGreSQL files
		try:
			engine = create_engine(name_or_url = 'postgresql://' + user + ':' + password + '@' + host + ':' + port + '/' + database)
			print('Connection Successful')

			contestants.to_sql('survivorContestantsSeason' + str(self.seasonNum), engine, schema = 'contestants', if_exists = 'replace', index = False)
			episodes.to_sql('survivorEpisodesSeason' + str(self.seasonNum), engine, schema = 'episodes', if_exists = 'replace', index = False)

		except Exception as ex:
			print('Connection Unsucessful', ex)

		return contestants, episodes







	








