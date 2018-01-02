import psycopg2

conn = psycopg2.connect(host="35.185.19.253",
                 database="crypto",
                 port="5432",
                 user="cryptogoats",
                 password="+tAVMsTH*2\30!fh")
print(conn)
