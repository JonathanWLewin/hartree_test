lam = lambda x: [y[:-1] + (x[1]['left'][0][-1] if x[1]['left'] != [] else 'NULL',) + (y[-1],) for y in x[1]['right']]

test = ('L1', {'left': [('L1', 85.0)], 'right': [('L1', 100.0)]})

print(lam(test))