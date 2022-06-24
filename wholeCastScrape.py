import requests
from bs4 import BeautifulSoup

import pandas as pd
import re


def makePersonList(name, season, person, half):
	# Create the list for each person to append to the main list
	entry = []

	# Get the persons name and add to list
	entry.append(name)

	# Add season number to list
	entry.append(season)

	# Add url to list
	bioURL = baseURL + person['href']

	bioData = requests.get(bioURL)
	bioHtml = BeautifulSoup(bioData.text, 'html.parser')


	## WORKING ON THIS paRT< nEed to clean the text and split for blood vs water seasons
	bioContent = bioHtml.find(class_ = 'cast-bio')


	if half == 0: # They are not part of a pair
		# Add url to list
		entry.append(bioContent)
	elif half == 1: # they are the first half of a pair
		split_string = re.split(r'<strong>Name:</strong>|<strong>Name \(Age\):</strong>|<strong>Name \(Age\)</strong>:|<strong>Name</strong>:|<strong>Name \(Age\): </strong>|<strong>Name: </strong>', str(bioContent))

		print(len(split_string), season)

		for string in split_string:
			print(string)

		entry.append(split_string[1])

	elif half == 2: # they are the second half of a pair
		split_string = re.split(r'<strong>Name:</strong>|<strong>Name \(Age\):</strong>|<strong>Name \(Age\)</strong>:|<strong>Name</strong>:|<strong>Name \(Age\): </strong>|<strong>Name: </strong>', str(bioContent))

		print(len(split_string), season)

		for string in split_string:
			print(string)

		entry.append(split_string[2])

	return entry


url = 'https://www.cbs.com/shows/survivor/cast/'

baseURL = 'https://www.cbs.com'

data = requests.get(url)
html = BeautifulSoup(data.text, 'html.parser')

# Get the current number of seasons based on how many links are in the drop down
numSeasons = len(html.find('li', class_ = 'pv-h').find('ul').findAll('li'))

# allCast is a list of lists, each list holding the cast members name, season number, and a link to their bio
allCast = []


# Seasons that are currently problems: 1 - 26 (only first names) and 27 and 29 (blood vs water season)

for season in range(1, numSeasons + 1):

	seasonUrl = url + 'season/' + str(season) + '/'

	seasonData = requests.get(seasonUrl)
	seasonHtml = BeautifulSoup(seasonData.text, 'html.parser')

	castList = seasonHtml.find('div', class_ = 'grid-view-container').findAll('a')

	for person in castList:
		# Make sure its not Jeff Probst
		name = str.strip(person.find('div', class_ = 'title').contents[0])

		# Remove mentors and some Jeffs
		meta = ''

		try:
			meta = person.find(class_ = 'meta-gray').contents[0].strip()
		except AttributeError:
			pass



		if (meta != 'Mentor') and 'Jeff Probst' not in name:

			half = 0

			if ' & ' in name:
				names = name.split(' & ')



				for name in names:
					half += 1

					allCast.append(makePersonList(name, season, person, half))

			elif ' and ' in name:
				names = name.split(' and ')

				for name in names:
					half += 1

					allCast.append(makePersonList(name, season, person, half))

			else:

				allCast.append(makePersonList(name, season, person, half))


df = pd.DataFrame(allCast, columns=['Name', 'Season Number', 'Link to Bio'])
df.to_csv('nameSeasonBio.csv', index=False)
