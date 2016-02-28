import numpy as np


def calc_duration(users_tutorial):
    durations = {}

    for user in users_tutorial:
        start_time = user[1]
        end_time = user[2]
        duration = int((end_time - start_time) / float(1000) / float(60))

        durations[duration] = durations.get(duration, []) + [1]

    durations_count = {}
    for k, v in durations.iteritems():
        durations_count[k] = len(v)

    return durations_count


if __name__ == '__main__':
    users = np.array([[1000001, 1454077665000, 1454077765000],
                      [1000002, 1454087665000, 1454097665000],
                      [1000006, 1454088665000, 1454098665000],
                      [1000007, 1454089665000, 1454099665000],
                      [1000008, 1454097665000, 1454097666000],
                      [1000009, 1454097766000, 1454097966000],
                      [1000010, 1454097866000, 1454097966000]])

    print calc_duration(users)
