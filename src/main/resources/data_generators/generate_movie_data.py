import os
import random


def generate_movie_records(f, user_id, delimiter):
    num_records = random.randint(1, 10)
    movie_ids = random.sample(range(0, 1000), num_records)
    gender = "m" if random.random() > 0.5 else "f"

    for i in range(num_records):
        score = random.randint(0, 100)
        record = str(user_id) + delimiter + gender + delimiter + str(movie_ids[i]) + delimiter + str(score) + "\n"
        f.write(record)

    return num_records


def generate(output_folder, prefix, delimiter=";", num_files=10, records_per_file=1000000):
    if num_files <= 0 or records_per_file <= 0:
        print "num_files and records_per_file should be greater than 0"
        return None
    if not os.path.isdir(output_folder):
        print "folder " + output_folder + " does not exist"
        return None

    i, user_id = 0, 0

    header = "userId" + delimiter + "gender" + delimiter + "movieId" + delimiter + "score\n"
    while i < num_files:
        filename = output_folder + os.sep + prefix + str(i) + ".csv"
        f = open(filename, "w")
        f.write(header)
        current_records = 0
        while current_records < records_per_file:
            current_records += generate_movie_records(f, user_id, delimiter)
            user_id += 1

        f.close()
        i += 1
    print "Done"


if __name__ == '__main__':
    output_folder = "/Users/hiphop/Downloads/movie_data/"
    prefix = "input"
    generate(output_folder, prefix)
