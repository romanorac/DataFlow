import os
import random
import time


def generate_movie_records(f, user_id, delimiter):
    num_records = random.randint(1, 2)
    current_millis = int(round(time.time() * 1000))

    start_offset = random.randint(1000, 10000000000)
    end_offset = random.randint(1000, 10000000000)

    ts_start = current_millis - start_offset
    ts_end = current_millis + end_offset

    for i in range(num_records):
        if i == 1:
            record = str(user_id) + delimiter + str(ts_start) + "\n"
        else:
            record = str(user_id) + delimiter + str(ts_end) + "\n"
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

    header = "userId" + delimiter + "timestamp\n"
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
    output_folder = "/Users/hiphop/Downloads/tutorial_data/"
    prefix = "input"
    generate(output_folder, prefix)
