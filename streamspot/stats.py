import sys
import csv

def analyze(filepath):
	"""
	Analyze "stats_streamspot.csv" file generated from parsing streamspot data.
	stats_streamspot.csv has the following format:
	graph_id,|V|,|E|
	ID 0 - 99: YouTube
	ID 100 - 199: Gmail
	ID 200 - 299: vgame
	ID 300 - 399: attack
	ID 400 - 499: download
	ID 500 - 599: CNN
	"""
	data = open(file_path, 'r')
	reader = csv.reader(data)

	average_num_node = 0.0
	average_num_edge = 0.0
	cnt = 0

	for row in reader:
		graph_id = row[0]
		num_node = row[1]
		num_edge = row[2]

		if int(graph_id) < 100:
			cnt += 1
			average_num_node = average_num_node + (int(num_node) - average_num_node) / cnt
			average_num_edge = average_num_edge + (int(num_edge) - average_num_edge) / cnt

			if int(graph_id) == 99:
				print "Average Number of Nodes (YouTube): " + str(average_num_node)
				print "Average Number of Edges (YouTube): " + str(average_num_edge)

				average_num_node = 0.0
				average_num_edge = 0.0
				cnt = 0

		elif int(graph_id) < 200:
			cnt += 1
			average_num_node = average_num_node + (int(num_node) - average_num_node) / cnt
			average_num_edge = average_num_edge + (int(num_edge) - average_num_edge) / cnt

			if int(graph_id) == 199:
				print "Average Number of Nodes (Gmail): " + str(average_num_node)
				print "Average Number of Edges (Gmail): " + str(average_num_edge)

				average_num_node = 0.0
				average_num_edge = 0.0
				cnt = 0

		elif int(graph_id) < 300:
			cnt += 1
			average_num_node = average_num_node + (int(num_node) - average_num_node) / cnt
			average_num_edge = average_num_edge + (int(num_edge) - average_num_edge) / cnt

			if int(graph_id) == 299:
				print "Average Number of Nodes (vgame): " + str(average_num_node)
				print "Average Number of Edges (vgame): " + str(average_num_edge)

				average_num_node = 0.0
				average_num_edge = 0.0
				cnt = 0

		elif int(graph_id) < 400:
			cnt += 1
			average_num_node = average_num_node + (int(num_node) - average_num_node) / cnt
			average_num_edge = average_num_edge + (int(num_edge) - average_num_edge) / cnt

			if int(graph_id) == 399:
				print "Average Number of Nodes (attack): " + str(average_num_node)
				print "Average Number of Edges (attack): " + str(average_num_edge)

				average_num_node = 0.0
				average_num_edge = 0.0
				cnt = 0

		elif int(graph_id) < 500:
			cnt += 1
			average_num_node = average_num_node + (int(num_node) - average_num_node) / cnt
			average_num_edge = average_num_edge + (int(num_edge) - average_num_edge) / cnt

			if int(graph_id) == 499:
				print "Average Number of Nodes (download): " + str(average_num_node)
				print "Average Number of Edges (download): " + str(average_num_edge)

				average_num_node = 0.0
				average_num_edge = 0.0
				cnt = 0

		elif int(graph_id) < 600:
			cnt += 1
			average_num_node = average_num_node + (int(num_node) - average_num_node) / cnt
			average_num_edge = average_num_edge + (int(num_edge) - average_num_edge) / cnt

			if int(graph_id) == 599:
				print "Average Number of Nodes (CNN): " + str(average_num_node)
				print "Average Number of Edges (CNN): " + str(average_num_edge)

				average_num_node = 0.0
				average_num_edge = 0.0
				cnt = 0

if __name__ == "__main__":
	if (len(sys.argv) < 2):
		print("""
			Usage: python stats.py <input_file>
		"""
		)
		sys.exit(1)
	analyze(sys.argv[1])
		