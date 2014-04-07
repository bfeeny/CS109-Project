from mrjob.job import MRJob
import re
import string

WORD_RE = re.compile(r"[\w']+")
 
class MRWordCountByReview(MRJob):

	def steps(self):
    		return [self.mr(mapper=self.mapper)]
 
	def mapper(self, _, line):
		# skip if this is the first/header line
		lnum = line.split(',',6)[0]
                if (lnum == ""):
                	return
		
		# remove punctuation
		line = line.translate(string.maketrans("",""), string.punctuation)

		yield (lnum.strip("\""), len(WORD_RE.findall(line)))

 
if __name__ == '__main__':
    MRWordCountByReview.run()
