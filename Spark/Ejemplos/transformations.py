# https://data-flair.trainihttps://data-flair.training/blogs/apache-spark-map-vs-flatmap/ng/blogs/apache-spark-map-vs-flatmap/

import pyspark as ps

conf = ps.SparkConf().setMaster("local[4]").setAppName("transformations")
sc = ps.SparkContext(conf=conf)


# MAP
# ---
rdd = sc.parallelize([1, 2, 3, 4])
rdd.map(lambda x : x * 2)
# Takes one element and produce one element

# FLATMAP
# -------
rdd = sc.parallelize([1, 2, 3])
rdd.map(lambda x: [x, x * 2])
# Output: [[1,2], [2, 4], [3, 6]]

rdd.flatMap(lambda x: [x, x * 2])
# Output: [1, 2, 2, 4, 3, 6]
# Takes one element and produces zero, one or more elements

#----------------------------------------------------------

# FILTER
# ------
rdd = sc.parallelize([1, 2, 3, 4])
rdd.filter(lambda x: x % 2 == 0)
# Output: [2, 4]

# Key-value Operations
pets = sc.parallelize([('cat', 1), ('dog', 1), ('cat', 2)])
pets.reduceByKey(lambda x, y: x + y)
# Output: [('cat', 3), ('dog', 1)]
pets.groupByKey()
# Output: [('cat', Seq(1, 2)), ('dog', Seq(1)]
pets.sortByKey()
# Output: [('cat', 1), ('cat', 2), ('dog', 1)]

#----------------------------------------------------------

# UNION
# -----
john_smith = [('physics', 85), ('maths', 75), ('chemistry', 95)]
paul_adams = [('physics', 65), ('maths', 45), ('chemistry', 85)]
john = sc.parallelize(john_smith)
paul = sc.parallelize(paul_adams)
john.union(paul).collect()
# [('physics', 85), ('maths', 75), ('chemistry', 95), ('physics', 65), ('maths', 45), ('chemistry', 85)]
# Merge two RDDS together

# JOIN
# ----
subject_wise_john = john.join(paul)
subject_wise_john.collect()
# Joins two RDD based on a common key

#----------------------------------------------------------

# INTERSECTION
# ------------
techs = ['sachin', 'abhay', 'michael', 'rahane', 'david', 'ross', 'raj', 'rahul', 'hussy', 'steven', 'sourav']
managers = ['rahul', 'abhay', 'laxman', 'bill', 'steve ']
techRDD = sc.parallelize(techs)
managersRDD = sc.parallelize(managers)
managerTechs = techRDD.intersection(managersRDD)
managerTechs.collect()
# Common terms


# DISTINCT
# --------
best_screenplay = ["movie10", "movie4", "movie6", "movie7", "movie3"]
best_story = ["movie9", "movie4", "movie6", "movie5", "movie1"]
best_direction = ["movie10", "movie4", "movie7", "movie12", "movie8"]
story_rdd = sc.parallelize(best_story)
direction_rdd = sc.parallelize(best_direction)
screen_rdd = sc.parallelize(best_screenplay)
total_nomination_rdd = story_rdd.union(direction_rdd).union(screen_rdd)
total_nomination_rdd.distinct().collect()
# Shows the distincts elements
