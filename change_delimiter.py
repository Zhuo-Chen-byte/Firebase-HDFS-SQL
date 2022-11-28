import csv

reader = csv.reader(open("winequality-white.csv", "r"), delimiter=';')
writer = csv.writer(open("winequality-white.csv-v1.csv", 'w'), delimiter=',')
writer.writerows(reader)

print("Delimiter successfully changed")

