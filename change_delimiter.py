import csv

reader = csv.reader(open("winequality-red.csv", "r"), delimiter=';')
writer = csv.writer(open("winequality-red-v1.csv", 'w'), delimiter=',')
writer.writerows(reader)

print("Delimiter successfully changed")

