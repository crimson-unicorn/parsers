import sys
import csv

def read_stat(file_path):
	"""
	Read statistics from a csv file and calculate the moving average of node and edge count.
	"""
	data = open(file_path, 'r')
	reader = csv.reader(data)

	average_num_node = 0.0
	average_num_edge = 0.0
	cnt = 0
	for row in reader:
		num_node = row[1]
		num_edge = row[2]
		cnt += 1
		average_num_node = average_num_node + (num_node - average_num_node) / cnt
		average_num_edge = average_num_edge + (num_edge - average_num_edge) / cnt
	data.close()
	return average_num_node, average_num_edge

if __name__ == "__main__":
	if (len(sys.argv) < 2):
		print("""
			Usage: python stats.py <input_file>
		"""
		)
		sys.exit(1)
	avg_node, avg_edge = read_stat(sys.argv[1])
	print "Average Number of Nodes: " + str(avg_node)
	print "Average Number of Edges: " + str(avg_edge)
