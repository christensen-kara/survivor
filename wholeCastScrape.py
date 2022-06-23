import requests
from bs4 import BeautifulSoup

import pandas as pd

url = 'https://www.cbs.com/shows/survivor/cast/'

baseURL = 'https://www.cbs.com'

data = requests.get(url)
html = BeautifulSoup(data.text, 'html.parser')

# Get the current number of seasons based on how many links are in the drop down
numSeasons = len(html.find('li', class_ = 'pv-h').find('ul').findAll('li'))

# allCast is a list of lists, each list holding the cast members name, season number, and a link to their bio
allCast = []

for season in range(1, numSeasons + 1):

	seasonUrl = url + 'season/' + str(season) + '/'

	seasonData = requests.get(seasonUrl)
	seasonHtml = BeautifulSoup(seasonData.text, 'html.parser')

	castList = seasonHtml.find('div', class_ = 'grid-view-container').findAll('a')

	for person in castList:
		# Make sure its not Jeff Probst
		name = str.strip(person.find('div', class_ = 'title').contents[0])

		if name != 'Jeff Probst':
			# Create the list for each person to append to the main list
			entry = []

			# Get the persons name and add to list
			entry.append(name)

			# Add season number to list
			entry.append(season)

			# Add url to list
			bioURL = baseURL + person['href']

			# bioData = requests.get(bioURL)
			# bioHtml = BeautifulSoup(bioData.text, 'html.parser')

			# Add url to list
			entry.append(bioURL)

			allCast.append(entry)


df = pd.DataFrame(allCast, columns=['Name', 'Season Number', 'Link to Bio'])
df.to_csv('nameSeasonBio.csv', index=False)
