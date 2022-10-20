import pandas as pd 
import numpy as np
import re
from sqlalchemy import create_engine
import psycopg2
import unicodedata
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import train_test_split
from sklearn.feature_selection import RFE
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder

from keras.models import Sequential
from keras.layers import Dense, InputLayer
from keras.utils.np_utils import to_categorical
from keras.callbacks import EarlyStopping
from keras.layers import Dropout
from keras.constraints import max_norm
from keras.wrappers.scikit_learn import KerasClassifier
from sklearn.model_selection import cross_val_score
from sklearn.datasets import make_classification
from sklearn.linear_model import LogisticRegression

from sklearn import metrics
from sklearn.naive_bayes import GaussianNB


def makeDuplDict():
	findDuplicates = episodes[['Episode', 'Eliminated', 'Season Number']]
	names = contestants[['Called', 'Season Number']]

	dupleDict = {}

	for i in range(1, max(names['Season Number']) + 1):
		seasonDict = {}

		currContestants = list(contestants[contestants['Season Number'] == i]['Called'])

		contWithDupl = [name for name in currContestants if bool(re.search(' [A-Z]\.', name))]

		for name in contWithDupl:
			firstName = name.split(' ')[0]
			personList = [otherName for otherName in contWithDupl if firstName in otherName and otherName != name]

			try:
				nameElim = int(findDuplicates[findDuplicates['Eliminated'] == name]['Episode'].values[0])
			except IndexError:
				nameElim = 16

			try:
				otherElim = int(findDuplicates[findDuplicates['Eliminated'] == personList[0]]['Episode'].values[0])
			except IndexError:
				otherElim = 16

			if nameElim > otherElim: 
				personList.append(True)
				personList.append(otherElim)
			else:
				personList.append(False)
				personList.append(otherElim)

			seasonDict[name] = personList

		dupleDict[i] = seasonDict

	return dupleDict



def findCount(called, season, totalDict, episode):
	if called in totalDict[season]:
		if totalDict[season][called][1] == True:
			if episode > totalDict[season][called][2]:
				return called[:-3]
			else:
				return called
		else:
			return called
	else:
		return called

def applyCount(row):
	currentSeason = episodes[episodes['Season Number'] == row['Season Number']].sort_values(by = ['Episode'], ascending = True)
	counts = []

	currentSeason.apply(lambda x: counts.append(x['Description'].count(findCount(row['Called'], row['Season Number'], dupleDict, x['Episode']))), axis = 1)

	return counts

def normal(x):
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

# Replace with your database credentials
user = 'USERNAME'
password = 'PASSWORD'
host = 'HOST'
port = 'PORT'
database = 'DATABASE'

engine = create_engine(name_or_url = 'postgresql://' + user + ':' + password + '@' + host + ':' + port + '/' + database)
contestants = pd.read_sql_table('allContestants', engine, schema = 'overall')
contestants['Called'] = contestants['Called'].apply(normal)
episodes = pd.read_sql_table('allEpisodes', engine, schema = 'overall')

dupleDict = makeDuplDict()




episodes = episodes[['Episode', 'Description', 'Season Number']].drop_duplicates(ignore_index = True)
episodes['Episode'] = episodes['Episode'].apply(int)
episodes['Season Number'] = episodes['Season Number'].apply(int)

contestants = contestants[['Name', 'Called', 'Season Number', 'Is Finalist?', 'Is Winner?']]
contestants['Season Number'] = contestants['Season Number'].apply(int)
contestants['Called'] = contestants['Called'].str.strip()

contestants = contestants[contestants['Season Number'] != 3]


contestants['Episode Counts'] = contestants.apply(applyCount, axis = 1)

maxLen = max(contestants['Episode Counts'].apply(len))
newColumns = list(range(1, maxLen + 1))

contestants[newColumns] = pd.DataFrame(contestants['Episode Counts'].tolist(), index = contestants.index)
contestants = contestants.fillna(0)


X = contestants[contestants['Is Finalist?'] == True][newColumns]
y = np.ravel(pd.get_dummies(contestants[contestants['Is Finalist?'] == True][['Is Winner?']]).values)

encoder = LabelEncoder()
encoder.fit(y)
encoded_y = encoder.transform(y)

avTrainScore = 0
avTestScore = 0
avCM = np.array([[0,0],[0,0]])

avTrainFeat = 0
avTestFeat = 0
avCMFeat = np.array([[0,0],[0,0]])

for i in range(1000):

	X_train, X_test, y_train, y_test = train_test_split(X, encoded_y, test_size = 0.3)



	# model = Sequential()
	# model.add(InputLayer(input_shape = X_train.shape[1]))
	# model.add(Dense(7, activation='relu'))
	# model.add(Dense(5, activation='relu'))
	# model.add(Dense(1, activation='sigmoid'))
	# model.compile(loss='binary_crossentropy', optimizer='adam',  metrics=['accuracy'])
	# model.summary()


	# epochs = 10
	# batch_size = 10
	# history = model.fit(X_train, y_train, epochs=epochs, batch_size=batch_size, validation_data=(X_test, y_test), callbacks=[EarlyStopping(monitor='val_loss', patience=3, min_delta=0.0001)])

	# _, train_acc = model.evaluate(X_train, y_train, verbose=0)
	# _, test_acc = model.evaluate(X_test, y_test, verbose=0)
	# print('Train: %.3f, Test: %.3f' % (train_acc, test_acc))
	# # plot history
	# plt.plot(history.history['loss'], label='train')
	# plt.plot(history.history['val_loss'], label='test')
	# plt.legend()
	# plt.show()


	gnb = GaussianNB()
	gnb.fit(X_train, y_train)
	  
	# making predictions on the testing set
	y_pred = gnb.predict(X_test)
	y_train_pred = gnb.predict(X_train)

	trainScore = metrics.accuracy_score(y_train, y_train_pred) * 100

	score = metrics.accuracy_score(y_test, y_pred)*100

	cm = metrics.confusion_matrix(y_test, y_pred)


	avTrainScore += trainScore
	avTestScore += score
	avCM += np.array(cm)
	# print(cm)

	# plt.figure(figsize=(9,9))
	# sns.heatmap(cm, annot=True, fmt=".3f", linewidths=.5, square = True, cmap = 'Blues_r');
	# plt.ylabel('Actual label');
	# plt.xlabel('Predicted label');
	# all_sample_title = 'Accuracy Score: {0}'.format(score)
	# plt.title(all_sample_title, size = 15);
	# plt.show()


	model = LogisticRegression() 
	rfe = RFE(model, n_features_to_select = 3)

	fit = rfe.fit(X_train, y_train)
	X_train = X_train.loc[:, rfe.support_]
	X_test = X_test.loc[:, rfe.support_]

	# model = Sequential()
	# model.add(InputLayer(input_shape = X_train.shape[1]))
	# model.add(Dense(3, activation='relu'))
	# model.add(Dense(2, activation='relu'))
	# model.add(Dense(1, activation='sigmoid'))
	# model.compile(loss='binary_crossentropy', optimizer='adam',  metrics=['accuracy'])
	# model.summary()

	# epochs = 10
	# batch_size = 10
	# history = model.fit(X_train, y_train, epochs=epochs, batch_size=batch_size, validation_data=(X_test, y_test), callbacks=[EarlyStopping(monitor='val_loss', patience=3, min_delta=0.0001)])

	# _, train_acc = model.evaluate(X_train, y_train, verbose=0)
	# _, test_acc = model.evaluate(X_test, y_test, verbose=0)
	# print('Train: %.3f, Test: %.3f' % (train_acc, test_acc))
	# # plot history
	# plt.plot(history.history['loss'], label='train')
	# plt.plot(history.history['val_loss'], label='test')
	# plt.legend()
	# plt.show()

	gnb = GaussianNB()
	gnb.fit(X_train, y_train)
	  
	# making predictions on the testing set
	y_pred = gnb.predict(X_test)
	y_train_pred = gnb.predict(X_train)

	trainScore = metrics.accuracy_score(y_train, y_train_pred) * 100

	score = metrics.accuracy_score(y_test, y_pred)*100

	cm = metrics.confusion_matrix(y_test, y_pred)

	avTrainFeat += trainScore
	avTestFeat += score
	avCMFeat += np.array(cm)

	# print(cm)

	# plt.figure(figsize=(9,9))
	# sns.heatmap(cm, annot=True, fmt=".3f", linewidths=.5, square = True, cmap = 'Blues_r');
	# plt.ylabel('Actual label');
	# plt.xlabel('Predicted label');
	# all_sample_title = 'Accuracy Score: {0}'.format(score)
	# plt.title(all_sample_title, size = 15);
	# plt.show()

# Look at just the finalists to compare finalists to winners
# Look at the first 6 episodes to compare non-finalists to finalists to winners
avTrainScore /= 1000
avTestScore /= 1000
avCM = np.divide(avCM, 1000.0)

avTrainFeat /= 1000
avTestFeat /= 1000
avCMFeat = np.divide(avCMFeat, 1000.0)

print('For all features:')
print('Training Score: ' + str(avTrainScore) + ' Test Score: ' + str(avTestScore) + 'Confusion Matrix: ' + str(avCM))

print()
print('For selected features:')
print('Training Score: ' + str(avTrainFeat) + ' Test Score: ' + str(avTestFeat) + 'Confusion Matrix: ' + str(avCMFeat))































