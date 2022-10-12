import pandas as pd 
import numpy as np
import re
from sqlalchemy import create_engine
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
from mpl_toolkits.axes_grid1.inset_locator import zoomed_inset_axes, mark_inset
import pyjags
from sklearn.preprocessing import LabelEncoder
from scipy.stats import norm, gamma

def twoWordStates(firstWord, secondWord):
	""" Ensures that all states are named correctly

		Accounts for two word states, by checking to see if the words captured make up the name of a state. If they do, it returns both words that form the state name. 
			Otherwise it just returns the one word that makes up the state name

		Args: 
			firstWord: A string containing what might be the first word of a two word state name, and is otherwise an unnecessary word
			secondWord: A string containing either the name of a state, or the second word in a two word state name

		Returns:
			A string containing a state name, regardless of if the state's name is one or two words

	"""
	# Store second words in two word state names based on the word that proceeds them
	newFirst = ['York', 'Jersey', 'Mexico', 'Hampshire']
	directionFirst = ['Dakota', 'Carolina']

	# Updates the value of word with the whole state name if necessary
	if secondWord in newFirst and firstWord == 'New':
		word = 'New '+ secondWord
	elif secondWord in directionFirst:
		word = firstWord + " " + secondWord
	elif secondWord == 'Virginia':
		if firstWord == 'West':
			word = 'West Virginia'
		else:
			word = 'Virginia'
	elif secondWord == 'Island':
		word = 'Rhode Island'
	else:
		word = secondWord

	return(word)

def updateStates(location, states):
	""" Updates counts of states and total

		Takes in where a contestant is from, then updates the count of people from that state or province, as well as the count of total number of contestants

		Args:
			location: A string holding where a contestant is from, in the format "City, State"
			states: A dictionary of where the keys are strings and the values are integers, holding a state name and the number of contestants from that state

	"""
	wordLocation = location.split() 
	word = twoWordStates(wordLocation[-2], wordLocation[-1]) 	# Save the states name based on the two final words of the location name

	# Update relevant values of states dictionary, including total which keeps track of all players
	states[word] += 1
	states['Total'] += 1

def calulateCounts(originalFrame, columnName, condition):
	""" Fills in a dictionary with counts of players from each state

		Finds all states that players are from, then turns it into a dictionary, which is updated so each state is paired with the number of players from that state. This
			count can be filtered so it only looks at players who are on the jury, who are finalists, and who are winners

		Args:
			originalFrame: A DataFrame holding the data to be explored, the contestants table
			columnName: A string containing the column name to be analyzed
			condition: A string containing the way we want the data to be subsetted

		Returns:
			A dictionary containing state names as keys and the number of players from that state as the values
			A list of all states that players are from

	"""

	# Creates a list of city and states and initializes the states as keys in a dictionary
	statePairs = list(originalFrame[columnName].str.replace(',', '').str.split()) 
	states = {}
	for statePair in statePairs:
		states[twoWordStates(statePair[-2], statePair[-1]).strip()] = 0
	stateList = states.keys()	# Save the names of the states in a list
	states['Total'] = 0

	try:
		if condition == "None": 
			originalFrame[columnName].apply(updateStates, states = states) 	# Finds the actual counts of players from the state
	except ValueError:	# If there is a value error, the computer could not compare the boolean to none, meaning there is a condition we need to filter the column name on 
						#	first
		originalFrame[condition][columnName].apply(updateStates, states = states)

	return states, stateList


def findAgeRange(age, ageDict):
	""" Updates a dictionary based on the age of a person

		Looks at the age difference from the mean of a player and adds it to the corresponding category, as well as updating the total number of players

		Args:
			age: An integer indicating the difference between a players age and the mean age of their season
			ageDict: A dictionary holding strings labeling different age ranges as keys and the number of players in that age range as values

	"""
	if age < -10:
		ageDict[ageRanges[0]] += 1
		ageDict['Total'] += 1

	elif age >= -10 and age < -5:
		ageDict[ageRanges[1]] += 1
		ageDict['Total'] += 1

	elif age >= -5 and age < 0:
		ageDict[ageRanges[2]] += 1
		ageDict['Total'] += 1

	elif age >= 0 and age < 5:
		ageDict[ageRanges[3]] += 1
		ageDict['Total'] += 1

	elif age >= 5 and age < 10:
		ageDict[ageRanges[4]] += 1
		ageDict['Total'] += 1

	elif age >= 10 and age < 15:
		ageDict[ageRanges[5]] += 1
		ageDict['Total'] += 1

	elif age >= 15 and age < 20:
		ageDict[ageRanges[6]] += 1
		ageDict['Total'] += 1

	elif age >= 20 and age < 25:
		ageDict[ageRanges[7]] += 1
		ageDict['Total'] += 1

	elif age >= 25 and age < 30:
		ageDict[ageRanges[8]] += 1
		ageDict['Total'] += 1

	elif age >= 30 and age < 35:
		ageDict[ageRanges[9]] += 1
		ageDict['Total'] += 1

	elif age >= 35 and age < 40:
		ageDict[ageRanges[10]] += 1
		ageDict['Total'] += 1

	else:
		ageDict[ageRanges[11]] += 1
		ageDict['Total'] += 1

def agePercentages(condition):
	""" Fills in the dictionary with a count of players from each age range

		Finds all age ranges that players are from, then turns it into a dictionary, which is updated so each age range is paired with the number of players from that age
			range. This count can be filtered so it only looks at players who are on the jury, who are finalists, and who are winners.

		Args:
			condition: A string containing the way we want the data to be subsetted

		Returns:
			A dictionary containing age range names as keys and the number of players from that age range as the values

	"""

	ageDict = {}
	for age in ageRanges:
		ageDict[age] = 0

	try:
		if condition == "None":
			contestants['Age Diff From Median'].apply(findAgeRange, ageDict = ageDict) # Finds the actual count of the players in each age range
	except ValueError:	# If there is a value error, the computer could not compare the boolean to none, meaning there is a condition we need to filter the column name on 
						#	first
		contestants[condition]['Age Diff From Median'].apply(findAgeRange, ageDict = ageDict)

	return ageDict

def my_autopct(pct):
	""" 
		Determines the format of the label on the pie chart. If the value is greater than 3 it will show its percentage value as a label on the pie chart. Otherwise it will
			be left blank.

	"""
	return ('%.2f' % pct) if pct > 3 else ''

# Replace these wth your credentials
user = 'USERNAME'
password = 'PASSWORD'
host = 'HOST'
port = '5432'
database = 'DATABASE'

engine = create_engine(name_or_url = 'postgresql://' + user + ':' + password + '@' + host + ':' + port + '/' + database)
contestants = pd.read_sql_table('allContestants', engine, schema = 'overall')

# For state question

# Creates dictionary holding states name and then the count of contestants from that state. Four dictionaries are created, one for all players, one for players who made the
#	jury, one for finalists, and one for winners
states, stateList = calulateCounts(contestants, 'From', 'None')
juryStates, stateList = calulateCounts(contestants, 'From', contestants['Is On Jury?'] == True)
finalistStates, stateList = calulateCounts(contestants, 'From', contestants['Is Finalist?'] == True)
winnerStates, stateList = calulateCounts(contestants, 'From', contestants['Is Winner?'] == True)

# Compiles and formats all data to find counts and percentages for all catagories into a list of lists and then a DataFrame
overallPercentages = [[state, states[state], states[state]/states['Total'] * 100, juryStates[state], juryStates[state]/juryStates['Total'] * 100, juryStates[state]/states[state] * 100, finalistStates[state], finalistStates[state]/finalistStates['Total'] * 100, finalistStates[state]/states[state] * 100, winnerStates[state], winnerStates[state]/winnerStates['Total'] * 100, winnerStates[state]/states[state] * 100] for state in stateList]
overallDataFrame = pd.DataFrame(overallPercentages, columns = ['State Name', 'Number of Contestants from State', 'Percentage of Contestants by State', 
	'Number of Jury Members By State', 'Percentage of Jury Members by State', 'Percentage of Jury Members Out of State Total', 
	'Number of Finalists by State', 'Percentages of Finalists by State', 'Percentage of Finalists Out of State Total', 'Number of Winners by State', 
	'Percentage of Winners by State', 'Percentage of Winners Out of State Total'])
overallDataFrame = overallDataFrame.sort_values(by = ['Number of Contestants from State'], ascending = False)
overallDataFrame.to_csv('stateQuestionsDataFrame.csv')

# Begin to make pie charts visualizing data found above
statePlot = overallDataFrame[overallDataFrame['State Name'] != 'Total']

# Pre-save data for each pie chart, one per catagory looked at (overall, jury members, finalists, winners)
colNames = ['Percentage of Contestants by State', 'Percentage of Jury Members by State', 'Percentages of Finalists by State', 'Percentage of Winners by State']
titles = ['Percent of Players From Each State/Province', 'Percent of Jury Members From Each State/Province', 'Percent of Finalists From Each State/Province', 'Percent of Winners From Each State/Province']
pngNames = ['playersByState.png', 'juryByState.png', 'finalistByState.png', 'winnersByState.png']
colorPairs = [[149, 164], [48, 164], [279, 164], [230, 164]]

for i in range(4):
	# Prepare a version of the data based on the current column we're charting
	statePlotFrame = statePlot.copy()
	statePlotFrame.loc[statePlotFrame[colNames[i]] < 1.5, 'State Name'] = 'Other States/Provinces'
	statePlotFrame = statePlotFrame.groupby('State Name')[colNames[i]].sum().reset_index()
	statePlotFrame = statePlotFrame.sort_values(by = [colNames[i]], ascending = False)
	legendLabel = statePlotFrame.apply(lambda row: row['State Name'] + ': ' + str(round(row[colNames[i]], 2)) + '%', axis = 1)
	numSec = len(list(set(list(statePlotFrame['State Name']))))
	colors = sns.diverging_palette(colorPairs[i][0], colorPairs[i][1], s = 69, l = 45, n = numSec)
	textprops = {"fontsize":12, "fontname":'Sathu'}

	# Create and show the figure
	fig1, ax1 = plt.subplots(figsize = (10, 10))
	wedges, labels, autopct = ax1.pie(statePlotFrame[colNames[i]], labels = statePlotFrame['State Name'],  autopct= my_autopct, colors = colors, textprops = textprops)
	plt.legend(legendLabel, bbox_to_anchor = (1.5, .75), fancybox=True, shadow=True)
	plt.setp(labels, fontsize=14)
	plt.title(titles[i], fontsize=22)
	plt.savefig(pngNames[i], bbox_inches='tight')
	plt.show()


# For age question

# Calculate and store the difference between each contestants age and the median age on their season
contestants['Age'] = contestants['Age'].apply(int)
medianAge = contestants.groupby(['Season Number'])['Age'].agg(np.median)
contestants['Age Diff From Median'] = contestants.apply(lambda row: row['Age'] - medianAge[row['Season Number']], axis = 1)

# Store names of relevant categories
ageRanges = ['Less than 10 Years Below Median', 'Between 10 and 5 Years Below Median', 'Within 5 Years Below the Median',
				'Within 5 Years Above the Median', 'Between 5 and 10 Years Above the Median', 'Between 10 and 15 Years Above the Median',
				'Between 15 and 20 Years Above the Median', 'Between 20 and 25 Years Above the Median', 'Between 25 and 30 Years Above the Median',
				'Between 30 and 35 Years Above the Median', 'Between 35 and 40 Years Above the Median', 'More that 40 Years Above the Median', 'Total']

# Creates dictionary holding age categories and then the count of contestants from that age category. Four dictionaries are created, one for all players, one for players who 
#	made the jury, one for finalists, and one for winners
ages = agePercentages('None')
juryAges = agePercentages(contestants['Is On Jury?'] == True)
finalistAges = agePercentages(contestants['Is Finalist?'] == True)
winnerAges = agePercentages(contestants['Is Winner?'] == True)

# Compiles and formats all data to find counts and percentages for all catagories into a list of lists and then a DataFrame
overallAgePercentages = [[ageR, ages[ageR], ages[ageR]/ages['Total'] * 100, juryAges[ageR], juryAges[ageR]/juryAges['Total'] * 100, juryAges[ageR]/ages[ageR] * 100, finalistAges[ageR], finalistAges[ageR]/finalistAges['Total'] * 100, finalistAges[ageR]/ ages[ageR] * 100, winnerAges[ageR], winnerAges[ageR]/winnerAges['Total'] * 100, winnerAges[ageR]/ages[ageR] * 100] for ageR in ageRanges]
ageDataFrame = pd.DataFrame(overallAgePercentages, columns = ['Difference From Median Season Age', 'Players From Age Group', 'Percentage Players from Age Group', 
	'Jury Members From Age Group', 'Percentage of Jury Members From Age Group', 'Percentage of Age Group that Make Jury', 
	'Finalists From Age Group', 'Percentage of Finalists From Age Group', 'Percentage of Age Group that Become Finalists', 
	'Winners From Age Group', 'Percentage of Winners From Age Group', 'Percentage of Age Group that Become Winners'])
ageDataFrame = ageDataFrame.sort_values(by = ['Players From Age Group'], ascending = False)
ageDataFrame.to_csv('ageQuestionDataFrame.csv')

# Begin to make pie charts visualizing data found above
agePlot = ageDataFrame[ageDataFrame['Difference From Median Season Age'] != 'Total']

# Pre-save data for each pie chart, one per catagory looked at (overall, jury members, finalists, winners)
colNames = ['Percentage Players from Age Group', 'Percentage of Jury Members From Age Group', 'Percentage of Finalists From Age Group', 'Percentage of Winners From Age Group']
titles = ['Percent of Players From Each Age Range', 'Percent of Jury Members From Each Age Range', 'Percentage of Finalists From Age Group', 'Percent of Winners From Each Age Range']
pngNames = ['playersByAgeRange.png', 'juryByAgeRange.png', 'finalistByAgeRange.png', 'winnersByAgeRange.png']
colorPairs = [[164, 48], [279, 48], [230, 48], [149, 48]]

for i in range(4):
	# Prepare a version of the data based on the current column we're charting
	agePlotFrame = agePlot.copy()
	agePlotFrame.loc[agePlotFrame[colNames[i]] < 1.5, 'Difference From Median Season Age'] = 'Other Ages'
	agePlotFrame = agePlotFrame.groupby('Difference From Median Season Age')[colNames[i]].sum().reset_index()
	agePlotFrame = agePlotFrame.sort_values(by = [colNames[i]], ascending = False)
	legendLabel = agePlotFrame.apply(lambda row: row['Difference From Median Season Age'] + ': ' + str(round(row[colNames[i]], 2)) + '%', axis = 1)
	numSec = len(list(set(list(agePlotFrame['Difference From Median Season Age']))))
	colors = sns.diverging_palette(colorPairs[i][0], colorPairs[i][1], s = 69, l = 45, n = numSec)
	textprops = {"fontsize":12, "fontname":'Sathu'}

	# Create and show the figure
	fig1, ax1 = plt.subplots(figsize = (10, 10))
	wedges, labels, autopct = ax1.pie(agePlotFrame[colNames[i]], labels = agePlotFrame['Difference From Median Season Age'],  autopct= my_autopct, colors = colors, textprops = textprops)
	plt.legend(legendLabel, loc = 'upper left', bbox_to_anchor = (1.5, .75), fancybox=True, shadow=True)
	plt.setp(labels, fontsize=14)
	plt.title(titles[i], fontsize=22)
	plt.savefig(pngNames[i], bbox_inches='tight')
	plt.show()

# Reshape the DataFrame to prepare for a catagorical point plot
agePlot = ageDataFrame.drop(['Players From Age Group', 'Percentage Players from Age Group', 'Jury Members From Age Group', 'Percentage of Jury Members From Age Group', 
	'Finalists From Age Group', 'Percentage of Finalists From Age Group', 'Winners From Age Group', 'Percentage of Winners From Age Group'], axis = 1)
meltAge = agePlot.melt('Difference From Median Season Age', value_vars = ['Percentage of Age Group that Make Jury', 'Percentage of Age Group that Become Finalists', 'Percentage of Age Group that Become Winners'])

# Create a plot that demonstrating how players from each age group fair at different stages of the game. This plot has two insets to make it easier to read 
pal = sns.color_palette('Paired')
pal = pal.as_hex()
pal = ['000000'] + pal


fig, ax = plt.subplots( figsize = (15, 10))
textprops = {"fontsize":12, "fontname":'Sathu'}
sns.pointplot(data = meltAge, x = "variable", y = "value", hue = "Difference From Median Season Age", palette = pal, textprops = textprops, ax = ax)
ax.lines[0].set_linestyle("dashdot")
ax.lines[0].set_linewidth(4)
plt.title('Percentage of Age Groups that Make Each Stage of Game', fontsize=22)

ax2 = plt.axes([0.475, 0.45, .1, .4])
sns.pointplot(data = meltAge, x = "variable", y = "value", hue = "Difference From Median Season Age", palette = pal, textprops = textprops, ax = ax2)
ax2.set_xlim([0.95, 1.05])
ax2.set_ylim([9, 24])
plt.legend(loc = 'upper left', bbox_to_anchor = (1.5, .75), fancybox=True, shadow=True)
ax2.get_legend().remove()
ax2.set_ylabel('')
ax2.set_xlabel('')
ax2.set_xticklabels([])
ax2.lines[0].set_linestyle("dashdot")
ax2.lines[0].set_linewidth(4)
mark_inset(ax, ax2, loc1=4, loc2=3, fc="none", ec="0.5")

ax3 = plt.axes([0.8, 0.15, .05, .4])
sns.pointplot(data = meltAge, x = "variable", y = "value", hue = "Difference From Median Season Age", palette = pal, textprops = textprops, ax = ax3)
ax3.set_xlim([1.95, 2.05])
ax3.set_ylim([-0.5, 11])
ax3.get_legend().remove()
ax3.set_ylabel('')
ax3.set_xlabel('')
ax3.set_xticklabels([])
ax3.lines[0].set_linestyle("dashdot")
ax3.lines[0].set_linewidth(4)
mark_inset(ax, ax3, loc1=2, loc2=4, fc="none", ec="0.5")
ax.set_xlabel('')
ax.set_ylabel('Percent')
plt.savefig('percentOfAgeGroup.png', bbox_inches='tight')
plt.show()











































