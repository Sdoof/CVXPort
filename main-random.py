
def func(ind):
    print('yes')
    return ind

print(all(func(i) for i in [1, 0, 1]))