from mrjob.job import MRJob

class MRReviewCount(MRJob):

	def mapper(self, _, line):
		gsid = line.split(',',5)[1]
		if (gsid == "\"gsid\""): 
			return
		yield (gsid, 1)

	def combiner(self, gsid, counts):
		yield (gsid, sum(counts))
 
	def reducer(self, gsid, counts):
		yield (gsid, sum(counts)) 
 
if __name__ == '__main__':
    MRReviewCount.run()
