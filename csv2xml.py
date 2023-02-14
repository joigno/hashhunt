
import csv


print('<feed>')
with open('data/eth-10000.csv', newline='') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
    for row in spamreader:
        #print(row[0])
        s = '''<doc>
    <title>%s</title>
    <url>https://etherscan.io/address/%s</url>
</doc>
''' % (row[0],row[0])
        if row[0] != 'address':
            print(s)
        #print(', '.join(row[1:2]))

print('</feed>')