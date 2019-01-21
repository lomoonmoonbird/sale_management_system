


def mykey(x):
    xx= x.split('-')
    xxx = "".join(xx)
    return xxx


# print (sorted(strings, key=mykey))


from collections import defaultdict
# strings = ['1', '3', '5', '1-1', '1-3', '1-20', '2-1', '2-5', '2-4']
strings = ['1', '3', '5', '4', '11', '10']
def custom_sort(strings):
    d = defaultdict(list)
    for i in strings:

        ii = i.split('-')
        if len(ii) < 2:
            d[int(ii[0])] = []
        elif len(ii) == 2:
            d[int(ii[0])].append(int(ii[1]))

        else:
            pass


    final = []

    for key, value in d.items():
        # print(value)
        d[key] = sorted(value)
    keys = list(d.keys())
    keys.sort()
    # print(keys)
    print(d)
    for dd in keys:
        # print(dd)
        value = d.get(dd)
        # print(type(d))
        # print(d, '-', dd, '---', value)

        print('ddddd', dd)
        key = dd
        if not value:
            print('keykeykey', key)
            final.append(str(key))
        else:
            final += [str(key) + "-"+ str(inner) for inner in value]
    # for key, value in d.items():

    print(final)
    return final

print(custom_sort(strings))





a = ['1', '2', '5', '8', '6', '1-2', '1-20', '1-3', '1-5']


