# Based on code at https://github.com/Yelp/dataset-examples/blob/master/positive_category_words/simple_global_positivity.py

import re
import string
import nltk
from nltk.stem import WordNetLemmatizer
from mrjob.job import MRJob

MINIMUM_OCCURENCES = 100

def avg_and_total(iterable):
        items = 0
        total = 0.0

        for item in iterable:
                total += int(item)
                items += 1

        return total / items, total

class PositiveWords(MRJob):

	def steps(self):
                return [self.mr(mapper=self.mapper, reducer=self.reducer)]
#              	return [self.mr(mapper=self.mapper)]

        
	def mapper(self, _, line):
		lnum,gsid,reviewer,postdate,stars,review = line.split(',',5)

                # skip if this is the first/header line
                if (gsid == "\"gsid\""): 
                        return

		# skip if this school is not rated
		if((stars == "0") or (stars == "99")):
#			print "Skipping ", stars
			return
                
                # lower case all review text
                review = review.lower()

                # remove html encodings
                review = review.replace('&amp;','').replace('&lt;','').replace('&gt;','').replace('&quot;','').replace('&#039;','').replace('&#034;','')

                # remove punctuation
                review = review.translate(string.maketrans("",""), string.punctuation)

 		# we are not going to remove stop words for bigrams
                # remove stop words
		# stopset = set(nltk.corpus.stopwords.words('english'))
                # review = [word for word in review.split() if not word in stopset]

		# Use nltk's lemmatizer to create word stems
		wnl = WordNetLemmatizer()
		review = [wnl.lemmatize(word) for word in review.split()]

		# Create bigrams
		bigrams = zip(review[0::2],review[1::2])+zip(review[1::2],review[2::2])

		# we are not going to limit word use for bigram analysis
		# Limit use of word to once per review
                # words = set(review)

                for bigram in bigrams:
                        yield bigram, stars

        def reducer(self, bigram, ratings):
                avg, total = avg_and_total(ratings)

                if total < MINIMUM_OCCURENCES:
                        return

                yield (int(avg * 100), total), bigram

if __name__ == "__main__":
        PositiveWords().run()
