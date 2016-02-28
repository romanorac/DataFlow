import numpy as np


def task1(users_array):
    group_by_users = {}
    for user in users_array:
        key = user[0]
        group_by_users[key] = group_by_users.get(key, []) + [1]

    mean, count = 0, 0
    for k, v in group_by_users.iteritems():
        mean += len(v)
        count += 1

    return mean / float(count)


def task2(users_array):
    group_by_gender = {}
    for user in users_array:
        key = user[1]
        value = user[0]
        if key not in group_by_gender or value not in group_by_gender[key]:
            group_by_gender[key] = group_by_gender.get(key, []) + [value]

    ptc_by_gender = {}
    suma = len(group_by_gender[1]) + len(group_by_gender[2])
    ptc_by_gender[1] = len(group_by_gender[1]) / float(suma)
    ptc_by_gender[2] = len(group_by_gender[2]) / float(suma)
    return ptc_by_gender


def task3(users_array):
    group_by_users = {1: {}, 2: {}}
    for user in users_array:
        key = user[0]
        gender = user[1]
        group_by_users[gender][key] = group_by_users[gender].get(key, []) + [1]

    mean_by_gender = {}
    for k, v in group_by_users.iteritems():
        mean_by_gender[k] = sum(np.sum(group_by_users[k].values())) / float(len(group_by_users[k]))

    return mean_by_gender


def t_test(x, y):
    x_avg = np.average(x)
    n_x = len(x)

    y_avg = np.average(y)
    n_y = len(y)

    var_x = variance(x, x_avg)
    var_y = variance(y, y_avg)

    return (x_avg - y_avg) / np.sqrt(var_x / n_x + var_y / n_y)


def variance(x, x_avg):
    return np.sum(np.square(x - x_avg)) / float(len(x) - 1)


def task4(users_array):
    mean_female, mean_male = [], []

    group_by_users = {1: {}, 2: {}}
    for user in users_array:
        key = user[0]
        gender = user[1]
        score = user[3]
        group_by_users[gender][key] = group_by_users[gender].get(key, []) + [score]

    for gender, users_scores in group_by_users.iteritems():
        for user_id, user_score in users_scores.iteritems():
            if gender == 1:
                # females
                mean_female.append(np.average(user_score))
            else:
                # males
                mean_male.append(np.average(user_score))
    t_score = t_test(mean_male, mean_female)
    return t_score


if __name__ == '__main__':
    # user_id, gender 1-female 2-male, movieId, movieScore
    users = np.array([[1, 2, 100, 50],
                      [1, 2, 101, 100],
                      [1, 2, 102, 0],
                      [2, 1, 100, 100],
                      [2, 1, 101, 80],
                      [3, 2, 104, 70],
                      [4, 2, 104, 80],
                      [5, 1, 100, 75],
                      [6, 2, 100, 60],
                      [6, 2, 101, 100],
                      [6, 2, 102, 80],
                      [7, 2, 104, 60],
                      [8, 1, 100, 70],
                      [8, 1, 101, 30],
                      [9, 2, 104, 70],
                      [10, 2, 111, 60],
                      [11, 1, 120, 65]])

    print task1(users)
    print task2(users)
    print task3(users)
    print task4(users)
