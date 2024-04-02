# Boilerplate stuff:
from pyspark import SparkConf, SparkContext
from dataset.utils import find_absolute_path

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf=conf)

# The characters we wish to find the degree of separation between:
start_character_id = 5306  # SpiderMan
target_character_id = 11

# Our accumulator, used to signal when we find the target character during
# our BFS traversal.
hit_counter = sc.accumulator(0)

COLOR_WHITE = 0
COLOR_GRAY = 1
COLOR_BLACK = 2


def to_bfs(line):
    fields = map(int, line.split())
    heroID, *connections = fields
    color = COLOR_WHITE
    distance = 9999

    if (heroID == start_character_id):
        color = COLOR_GRAY
        distance = 0

    return (heroID, (connections, distance, color))


def create_starting_rdd():
    input_file = sc.textFile(find_absolute_path("Marvel_Graph.txt"))
    return input_file.map(to_bfs)


def bfs_flat_map(node):
    character_id, (connections, distance, color) = node
    results = []

    # If this node needs to be expanded...
    if (color == COLOR_GRAY):
        for connection in connections:
            new_character_id = connection
            new_distance = distance + 1
            new_color = COLOR_GRAY
            if (target_character_id == connection):
                hit_counter.add(1)

            newEntry = (new_character_id, ([], new_distance, new_color))
            results.append(newEntry)
        # We've processed this node, so color it black
        color = COLOR_BLACK

    # Emit the input node so we don't lose it.
    results.append((character_id, (connections, distance, color)))
    return results


def bfs_reduce(data1, data2):
    edges1, distance1, color1 = data1
    edges2, distance2, color2 = data2

    # See if one is the original node with its connections.
    edges = list(set(edges1).union(edges2))
    # Preserve minimum distance
    distance = min(distance1, distance2)
    # Preserve darkest color
    color = max(color1, color2)
    return (edges, distance, color)


def continue_searching(iteration_rdd):
    return iteration_rdd.filter(lambda node: node[1][2] == COLOR_GRAY).count() > 0


# Main program here:
iteration_rdd = create_starting_rdd()

i = 0
is_hit = False
while continue_searching(iteration_rdd):
    i += 1
    print("Running BFS iteration # " + str(i))

    # Create new vertices as needed to darken or reduce distances in the
    # reduce stage. If we encounter the node we're looking for as a GRAY
    # node, increment our accumulator to signal that we're done.
    flat_mapped = iteration_rdd.flatMap(bfs_flat_map)

    print("Processing " + str(flat_mapped.count()) + " values.")

    # Only print for the first time we hit the target character.
    if (is_hit == False and hit_counter.value > 0):
        print(
            f"Hit the target character:{target_character_id}, from {str(i)} different direction(s).")
        is_hit = True

    # Reducer combines data for each character ID, preserving the darkest
    # color and shortest path.
    iteration_rdd = flat_mapped.reduceByKey(bfs_reduce)

print(f"Finish all search after {str(i)} different direction(s).")


# iteration_rdd.saveAsTextFile(".output")
