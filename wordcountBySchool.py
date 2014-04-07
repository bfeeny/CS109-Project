from mrjob.job import MRJob
import string
import nltk
import itertools
 
class MRWordCountBySchool(MRJob):

	def steps(self):
    		return [self.mr(mapper=self.mapper, reducer=self.reducer)]
#		return [self.mr(mapper=self.mapper)]

	def mapper(self, _, line):
		lnum,gsid,reviewer,postdate,stars,review = line.split(',',5)
		# print lnum,gsid,reviewer,postdate,stars,review

		# skip if this is the first/header line
                if (gsid == "\"gsid\""): 
                        return
		
		# lower case all review text
		review = review.lower()

		# remove html encodings
		review = review.replace('&amp;','').replace('&lt;','').replace('&gt;','').replace('&quot;','').replace('&#039;','').replace('&#034;','')

		# remove punctuation
		review = review.translate(string.maketrans("",""), string.punctuation)

                # remove stop words
		stopset = set(nltk.corpus.stopwords.words('english'))
		review = [word for word in review.split() if not word in stopset]

		# yield gsid and list of words
		for word in review:
			yield ((gsid,word),1)

        def reducer(self, key, counts):
		gsid,word = key
		count = [count for count in counts]
#                yield ((gsid,word), sum(count))
#		yield gsid, count
 		print gsid, word, sum(count)

if __name__ == '__main__':
    MRWordCountBySchool.run()
