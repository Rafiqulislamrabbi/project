import csv


tags = []
with open(
"tags.csv"
) as csvfile:  # Get all teg from csv file.
    reader = csv.DictReader(csvfile)
    for row in reader:
        y = row["tagName"]
        if int(row["count"]) < 1000:
            break
        tags.append(y)

