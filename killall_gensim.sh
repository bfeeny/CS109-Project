kill -9 `ps -eaf | grep "gensim.models\|Pyro" | grep -v grep | awk '{print $2}'`
